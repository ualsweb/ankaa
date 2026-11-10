//! Read-only PostgreSQL wire protocol front end (PRD F-21, experimental).
//!
//! Built on `datafusion-postgres` + `pgwire`: `psql`, JDBC/ODBC drivers and BI tools connect
//! with SCRAM-SHA-256 authentication (user name is free, the password is the server token) and
//! run SQL over the same `SessionContext` as Arrow Flight. A [`ReadOnlyHook`] rejects every
//! statement that is not a `SELECT`/`WITH`/`EXPLAIN` before `DataFusion` sees it, so the session
//! cannot be modified through this interface. For bulk result transfer use Arrow Flight; this
//! path encodes rows as Postgres text/binary and is meant for interactive and tooling access.
#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, Set, SetExpr, Statement};
use datafusion_postgres::DfSessionService;
use datafusion_postgres::hooks::{HookClient, QueryHook};
use datafusion_postgres::pgwire;
use pgwire::api::auth::sasl::SASLAuthStartupHandler;
use pgwire::api::auth::sasl::scram::{ScramAuth, gen_salted_password};
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password};
use pgwire::api::{ClientInfo, PgWireServerHandlers};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use tokio::net::TcpListener;

/// Crate version string (from Cargo).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// SCRAM iteration count (Postgres default).
const SCRAM_ITERATIONS: usize = 4096;

/// Rejects anything that is not a read-only query. Runs **first**, before the default hooks, and
/// checks both the syntax tree and the planned `LogicalPlan` (`DataFusion`'s `SQLOptions`, the same
/// gate Arrow Flight uses), so `SELECT … INTO`, `EXPLAIN ANALYZE <write>`, `COPY`, DDL/DML and
/// `SET datafusion.*` on the shared session are all refused. A small allowlist of Postgres
/// client-compatibility `SET` variables is passed on to the default `SET/SHOW` hook.
#[derive(Debug, Default)]
pub struct ReadOnlyHook;

/// `SET` variables tools send on connect. They never reach the `DataFusion` session, which is
/// shared with every other client: the ones the compatibility hook keeps per connection are
/// passed to it, the rest are acknowledged and ignored (see [`set_is_connection_local`]).
const SET_ALLOWLIST: &[&str] = &[
    "application_name",
    "client_encoding",
    "datestyle",
    "extra_float_digits",
    "search_path",
    "statement_timeout",
    "timezone",
    "intervalstyle",
    "client_min_messages",
    "standard_conforming_strings",
];

/// Variables the `SET/SHOW` compatibility hook stores in the connection's own metadata.
const CONNECTION_LOCAL: &[&str] = &[
    "statement_timeout",
    "datestyle",
    "intervalstyle",
    "application_name",
    "extra_float_digits",
    "search_path",
];

/// A query body that only reads: `WITH … INSERT/UPDATE/DELETE` and friends are bodies too.
fn body_reads_only(body: &SetExpr) -> bool {
    match body {
        SetExpr::Select(sel) => sel.into.is_none(),
        SetExpr::Query(q) => body_reads_only(&q.body),
        SetExpr::SetOperation { left, right, .. } => {
            body_reads_only(left) && body_reads_only(right)
        }
        SetExpr::Values(_) | SetExpr::Table(_) => true,
        _ => false,
    }
}

fn is_read_only(st: &Statement) -> bool {
    match st {
        Statement::Query(q) => body_reads_only(&q.body),
        // EXPLAIN ANALYZE executes the inner statement; plain EXPLAIN only plans it
        Statement::Explain {
            analyze, statement, ..
        } => is_read_only(statement) && (!*analyze || matches!(**statement, Statement::Query(_))),
        Statement::ExplainTable { .. }
        | Statement::ShowVariable { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowSchemas { .. }
        | Statement::ShowDatabases { .. }
        | Statement::ShowFunctions { .. } => true,
        Statement::Set(_) => set_variable_allowed(st),
        st if is_session_noise(st) => true,
        _ => false,
    }
}

/// Statements drivers and ORMs send around their queries that neither read nor write data:
/// transaction control (answered by the transaction hook; there is nothing to commit on a
/// read-only server, but psycopg, `SQLAlchemy` and JDBC without auto-commit wrap every query in
/// `BEGIN … COMMIT`) and connection housekeeping (acknowledged here).
fn is_session_noise(st: &Statement) -> bool {
    is_transaction_control(st) || housekeeping_tag(st).is_some()
}

fn is_transaction_control(st: &Statement) -> bool {
    matches!(
        st,
        Statement::StartTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
            | Statement::Savepoint { .. }
            | Statement::ReleaseSavepoint { .. }
    )
}

/// Command tag for a housekeeping statement that is acknowledged without doing anything.
fn housekeeping_tag(st: &Statement) -> Option<&'static str> {
    match st {
        Statement::Discard { .. } => Some("DISCARD ALL"),
        Statement::Deallocate { .. } => Some("DEALLOCATE"),
        _ => None,
    }
}

fn set_variable_allowed(st: &Statement) -> bool {
    match st {
        Statement::Set(Set::SingleAssignment {
            hivevar: false,
            variable,
            ..
        }) => SET_ALLOWLIST.contains(&variable.to_string().to_ascii_lowercase().as_str()),
        Statement::Set(
            Set::SetTimeZone { .. }
            | Set::SetNames { .. }
            | Set::SetNamesDefault {}
            | Set::SetTransaction { .. },
        ) => true,
        _ => false,
    }
}

/// True when the compatibility hook handles this `SET` entirely in the connection's metadata.
/// Any other form would fall through, inside that hook, to `DataFusion`'s own `SET`, which
/// writes the configuration of the session every client shares (`SET timezone = …` maps to
/// `datafusion.execution.time_zone`): those are answered here instead and change nothing.
fn set_is_connection_local(st: &Statement) -> bool {
    match st {
        Statement::Set(Set::SingleAssignment {
            scope: None,
            hivevar: false,
            variable,
            values,
        }) => {
            let var = variable.to_string().to_ascii_lowercase();
            var == "statement_timeout"
                || (CONNECTION_LOCAL.contains(&var.as_str())
                    && matches!(values.first(), Some(SqlExpr::Value(_))))
        }
        Statement::Set(Set::SetTimeZone { local: false, .. }) => true,
        _ => false,
    }
}

fn denied(st: &Statement, why: &str) -> PgWireError {
    let kind = st.to_string();
    let kind = kind
        .split_whitespace()
        .take(2)
        .collect::<Vec<_>>()
        .join(" ");
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "42501".to_owned(), // insufficient_privilege
        format!("ANKAA is read-only over the Postgres protocol: {kind} rejected ({why})"),
    )))
}

fn read_only_options() -> datafusion::prelude::SQLOptions {
    datafusion::prelude::SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

/// Plan `st` and verify the plan is read-only. `None` = allowed, `Some(err)` = rejected.
async fn check_statement(st: &Statement, ctx: &SessionContext) -> Option<PgWireError> {
    if !is_read_only(st) {
        return Some(denied(st, "statement type"));
    }
    if matches!(
        st,
        Statement::Set(_) | Statement::ShowVariable { .. } | Statement::ShowVariables { .. }
    ) || is_session_noise(st)
    {
        return None; // handled by the compatibility hooks, never planned by DataFusion
    }
    let df_stmt = datafusion::sql::parser::Statement::Statement(Box::new(st.clone()));
    match ctx.state().statement_to_plan(df_stmt).await {
        Ok(plan) => read_only_options()
            .verify_plan(&plan)
            .err()
            .map(|e| denied(st, &e.to_string())),
        // fail closed: a statement that cannot be planned here is not handed on to a path
        // that plans it again without the read-only check
        Err(e) => Some(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            e.to_string(),
        )))),
    }
}

#[async_trait::async_trait]
impl QueryHook for ReadOnlyHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        ctx: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<pgwire::api::results::Response>> {
        if let Some(e) = check_statement(statement, ctx).await {
            return Some(Err(e));
        }
        if matches!(statement, Statement::Set(_)) && !set_is_connection_local(statement) {
            // allowed for compatibility, but it must not reach the shared session: acknowledge
            return Some(Ok(pgwire::api::results::Response::Execution(
                pgwire::api::results::Tag::new("SET"),
            )));
        }
        if let Some(tag) = housekeeping_tag(statement) {
            return Some(Ok(pgwire::api::results::Response::Execution(
                pgwire::api::results::Tag::new(tag),
            )));
        }
        None
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        ctx: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<datafusion::logical_expr::LogicalPlan>> {
        // allowed SETs get their (empty) plan from the compatibility hook; what happens when
        // they are executed is decided in `handle_extended_query` below
        check_statement(statement, ctx).await.map(Err)
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        plan: &datafusion::logical_expr::LogicalPlan,
        _params: &datafusion::common::ParamValues,
        _ctx: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<pgwire::api::results::Response>> {
        if !is_read_only(statement) {
            return Some(Err(denied(statement, "statement type")));
        }
        if matches!(statement, Statement::Set(_)) && !set_is_connection_local(statement) {
            // acknowledged, never executed against the shared session
            return Some(Ok(pgwire::api::results::Response::Execution(
                pgwire::api::results::Tag::new("SET"),
            )));
        }
        if let Some(tag) = housekeeping_tag(statement) {
            return Some(Ok(pgwire::api::results::Response::Execution(
                pgwire::api::results::Tag::new(tag),
            )));
        }
        if matches!(statement, Statement::Set(_)) || is_transaction_control(statement) {
            return None;
        }
        read_only_options()
            .verify_plan(plan)
            .err()
            .map(|e| Err(denied(statement, &e.to_string())))
    }
}

/// A fresh 16-byte SCRAM salt per process. The salt is public (the server sends it to every
/// client); it only has to differ between servers so that a verifier computed for one is
/// useless against another. The standard library's hasher keys come from the OS generator.
fn random_salt() -> Vec<u8> {
    use std::hash::{BuildHasher, Hasher};
    let mut salt = Vec::with_capacity(16);
    for i in 0..2u8 {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u8(i);
        salt.extend_from_slice(&h.finish().to_le_bytes());
    }
    salt
}

/// Single-secret auth source: any user name, password = token (SCRAM salted verifier).
#[derive(Debug)]
struct TokenAuth {
    salt: Vec<u8>,
    salted: Vec<u8>,
}

#[async_trait::async_trait]
impl AuthSource for TokenAuth {
    async fn get_password(&self, _login: &LoginInfo) -> PgWireResult<Password> {
        Ok(Password::new(Some(self.salt.clone()), self.salted.clone()))
    }
}

/// pgwire handler set: SCRAM startup + read-only `DataFusion` session. The SCRAM handler keeps
/// per-connection state, so a fresh one is built for every connection.
pub struct PgHandlers {
    service: Arc<DfSessionService>,
    auth: Arc<TokenAuth>,
    params: Arc<DefaultServerParameterProvider>,
}

impl std::fmt::Debug for PgHandlers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PgHandlers")
    }
}

impl PgHandlers {
    /// Build handlers for `ctx`; `token` is the shared secret clients send as the password.
    #[must_use]
    pub fn new(ctx: Arc<SessionContext>, token: &str) -> Self {
        // read-only gate first; no cursor hook (it executes `DECLARE … FOR <query>` unchecked)
        let hooks: Vec<Arc<dyn QueryHook>> = vec![
            Arc::new(ReadOnlyHook),
            Arc::new(datafusion_postgres::hooks::set_show::SetShowHook),
            Arc::new(datafusion_postgres::hooks::transactions::TransactionStatementHook),
        ];
        let service = Arc::new(DfSessionService::new_with_hooks(ctx, hooks));
        let salt = random_salt();
        let auth = Arc::new(TokenAuth {
            salted: gen_salted_password(token, &salt, SCRAM_ITERATIONS),
            salt,
        });
        Self {
            service,
            auth,
            params: Arc::new(DefaultServerParameterProvider::default()),
        }
    }
}

impl PgWireServerHandlers for PgHandlers {
    fn simple_query_handler(&self) -> Arc<impl pgwire::api::query::SimpleQueryHandler> {
        self.service.clone()
    }
    fn extended_query_handler(&self) -> Arc<impl pgwire::api::query::ExtendedQueryHandler> {
        self.service.clone()
    }
    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        let mut scram = ScramAuth::new(self.auth.clone());
        scram.set_iterations(SCRAM_ITERATIONS);
        Arc::new(SASLAuthStartupHandler::new(self.params.clone()).with_scram(scram))
    }
}

/// Connections served at once; more are refused (closed) until one ends.
pub const MAX_CONNECTIONS: usize = 64;

/// Accept connections on `listener` until `shutdown` resolves. Plain TCP (put it behind a
/// management interface or a TLS terminator; TLS here is a follow-up).
///
/// At most [`MAX_CONNECTIONS`] are served at once, and a failed `accept` (for example when the
/// process is out of file descriptors) is retried after a pause instead of ending the listener.
/// There is no limit on how long one statement may run here (the query memory pool still
/// applies): expose this port to trusted tools only and use Arrow Flight, which enforces
/// `server.query_timeout_s`, for everything else.
pub async fn serve<F>(
    listener: TcpListener,
    handlers: Arc<PgHandlers>,
    shutdown: F,
) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()>,
{
    let slots = Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (socket, peer) = match accepted {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::warn!(error = %e, "pg accept failed");
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let Ok(permit) = slots.clone().try_acquire_owned() else {
                    tracing::warn!(%peer, "pg connection refused: too many connections");
                    continue;
                };
                // small request/response messages: do not wait for delayed ACKs
                let _ = socket.set_nodelay(true);
                let h = handlers.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(e) = pgwire::tokio::process_socket(socket, None, h).await {
                        tracing::debug!(%peer, error = %e, "pg connection ended");
                    }
                });
            }
        }
    }
}

/// Convenience: bind `addr` and serve.
pub async fn bind_and_serve<F>(
    addr: SocketAddr,
    handlers: Arc<PgHandlers>,
    shutdown: F,
) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()>,
{
    let listener = TcpListener::bind(addr).await?;
    serve(listener, handlers, shutdown).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn parse(sql: &str) -> Statement {
        Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .remove(0)
    }

    #[test]
    fn scram_salt_differs_between_processes_and_handlers() {
        let (a, b) = (random_salt(), random_salt());
        assert_eq!(a.len(), 16);
        assert_ne!(a, b);
        assert_ne!(a[..8], a[8..]);
    }

    #[test]
    fn read_only_classification() {
        for ok in [
            "SELECT 1",
            "WITH t AS (SELECT 1) SELECT * FROM t",
            "EXPLAIN SELECT 1",
            "SHOW TABLES",
        ] {
            assert!(is_read_only(&parse(ok)), "{ok}");
        }
        // what drivers wrap queries in
        for ok in [
            "BEGIN",
            "START TRANSACTION",
            "COMMIT",
            "ROLLBACK",
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
            "SET NAMES 'UTF8'",
            "DISCARD ALL",
            "DEALLOCATE ALL",
        ] {
            assert!(is_read_only(&parse(ok)), "{ok}");
        }
        // SET: only the compatibility variables, and only the forms kept per connection go on
        for (sql, allowed, local) in [
            ("SET application_name = 'psql'", true, true),
            ("SET extra_float_digits = 3", true, true),
            ("SET statement_timeout = '5s'", true, true),
            ("SET TIME ZONE 'UTC'", true, true),
            ("SET timezone = 'UTC'", true, false),
            ("SET timezone TO 'Europe/Madrid'", true, false),
            ("SET SESSION timezone = 'UTC'", true, false),
            ("SET client_encoding = 'UTF8'", true, false),
            ("SET search_path = public", true, false),
            ("SET datafusion.execution.batch_size = 1", false, false),
            ("SET timezone_foo = 1", false, false),
        ] {
            let st = parse(sql);
            assert_eq!(set_variable_allowed(&st), allowed, "{sql}");
            assert_eq!(allowed && set_is_connection_local(&st), local, "{sql}");
        }
        for bad in [
            "DROP TABLE trades",
            "CREATE TABLE t (a int)",
            "INSERT INTO trades VALUES (1)",
            "DELETE FROM trades",
            "UPDATE trades SET price = 0",
            "COPY trades TO '/tmp/x'",
            "CREATE VIEW v AS SELECT 1",
            "SET datafusion.execution.batch_size = 1",
            "EXPLAIN ANALYZE COPY (SELECT 1 AS a) TO '/tmp/x.csv'",
            "EXPLAIN COPY (SELECT 1 AS a) TO '/tmp/x.csv'",
            "EXPLAIN ANALYZE INSERT INTO trades VALUES (1)",
            "SELECT * INTO t FROM trades",
            "SET datafusion.execution.batch_size = 1",
        ] {
            assert!(!is_read_only(&parse(bad)), "{bad}");
        }
    }
}
