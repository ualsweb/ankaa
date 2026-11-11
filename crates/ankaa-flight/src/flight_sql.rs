//! Flight SQL on the plain Flight service: statements, prepared statements and catalog metadata.
//!
//! A Flight SQL client wraps every command in a protobuf `Any` whose type URL starts with
//! `type.googleapis.com/arrow.flight.protocol.sql.`. The service looks for that in descriptors,
//! tickets and action bodies ([`command_of`], [`any_of`]) and hands the message to this module;
//! everything else stays plain Flight.
//!
//! Nothing is kept per client. The ticket of a statement and the handle of a prepared statement
//! are the SQL text itself, so there is no table of open statements to bound, expire or lose at
//! a restart, and a handle gives a client nothing a query would not. The text is planned and
//! executed by the functions plain Flight uses (`AnkaaFlight::plan`, `AnkaaFlight::run_query`),
//! which is where the read-only check, the query timeout, the query slots and the memory pool
//! are applied.
//!
//! The server is read-only: updates, bulk ingest, transactions and Substrait plans are refused.
//! Statement parameters are refused as well: binding them would need a planning path of its own
//! beside the plan cache.
//!
//! All input here is untrusted: every decode returns a `Status`, nothing is indexed or unwrapped.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::{SchemaRef, UInt32Type};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::sql::metadata::SqlInfoDataBuilder;
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, Any, Command, CommandGetSqlInfo, ProstMessageExt, SqlInfo,
    SqlSupportedCaseSensitivity, SqlSupportedTransaction, TicketStatementQuery,
};
use arrow_flight::{
    Action, ActionType, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, IpcMessage,
    SchemaAsIpc, Ticket,
};
use datafusion::datasource::TableType;
use futures::TryStreamExt;
use prost::Message;
use tonic::Status;

use crate::{AnkaaFlight, BoxStream};

/// Every Flight SQL message type lives under this type URL prefix.
const TYPE_URL_PREFIX: &str = "type.googleapis.com/arrow.flight.protocol.sql.";

const CREATE_PREPARED_STATEMENT: &str = "CreatePreparedStatement";
const CLOSE_PREPARED_STATEMENT: &str = "ClosePreparedStatement";

/// Actions of the Flight SQL specification that this server refuses, with the reason.
const REFUSED_ACTIONS: &[(&str, &str)] = &[
    (
        "CreatePreparedSubstraitPlan",
        "Substrait plans are not supported",
    ),
    (
        "BeginTransaction",
        "transactions are not supported (the server is read-only)",
    ),
    (
        "EndTransaction",
        "transactions are not supported (the server is read-only)",
    ),
    (
        "BeginSavepoint",
        "transactions are not supported (the server is read-only)",
    ),
    (
        "EndSavepoint",
        "transactions are not supported (the server is read-only)",
    ),
    (
        "CancelQuery",
        "queries are not cancelled by request: they end at the server's query timeout or when the client drops the stream",
    ),
];

const READ_ONLY: &str = "ANKAA is read-only over Flight SQL: updates and bulk ingest are refused (data arrives through the configured sources)";

const NO_PARAMETERS: &str =
    "flight sql: statement parameters ($1, ?) are not supported; write the values in the SQL text";

/// A statement with parameters is refused as an invalid argument, not as unimplemented: the
/// ADBC DB-API layer takes "unimplemented" from a prepare as "this server has no prepared
/// statements", goes on without one and fails later with an error of its own.
fn no_parameters() -> Status {
    Status::invalid_argument(NO_PARAMETERS)
}

/// The Flight SQL message in `bytes`, when they hold one.
///
/// SQL text and WAL tickets never qualify: they would have to start with the encoding of a
/// protobuf string field holding the type URL prefix.
pub(crate) fn any_of(bytes: &[u8]) -> Option<Any> {
    let any = Any::decode(bytes).ok()?;
    any.type_url.starts_with(TYPE_URL_PREFIX).then_some(any)
}

/// The Flight SQL message in a `CMD` descriptor, when it holds one.
pub(crate) fn command_of(descriptor: &FlightDescriptor) -> Option<Any> {
    if descriptor.r#type == DescriptorType::Cmd as i32 {
        any_of(&descriptor.cmd)
    } else {
        None
    }
}

/// The actions `ListActions` announces.
pub(crate) fn action_types() -> Vec<ActionType> {
    vec![
        ActionType {
            r#type: CREATE_PREPARED_STATEMENT.to_owned(),
            description: "Flight SQL: prepare a read-only statement without parameters \
                          (ActionCreatePreparedStatementRequest -> ActionCreatePreparedStatementResult)"
                .to_owned(),
        },
        ActionType {
            r#type: CLOSE_PREPARED_STATEMENT.to_owned(),
            description: "Flight SQL: close a prepared statement \
                          (ActionClosePreparedStatementRequest)"
                .to_owned(),
        },
    ]
}

/// Why a `DoPut` carrying this Flight SQL command is refused. Nothing is written through Flight.
pub(crate) fn refuse_put(any: Any) -> Status {
    match command(any) {
        Err(status) => status,
        Ok(
            Command::CommandStatementUpdate(_)
            | Command::CommandPreparedStatementUpdate(_)
            | Command::CommandStatementIngest(_),
        ) => Status::unimplemented(READ_ONLY),
        // a client binds the parameters of a prepared query with a DoPut
        Ok(Command::CommandPreparedStatementQuery(_)) => no_parameters(),
        Ok(other) => unsupported(other.type_url()),
    }
}

fn command(any: Any) -> Result<Command, Status> {
    Command::try_from(any)
        .map_err(|e| Status::invalid_argument(format!("flight sql: malformed command: {e}")))
}

/// Status for a Flight SQL message this server does not implement. The name comes from the
/// client: only a bounded part of it is echoed.
fn unsupported(type_url: &str) -> Status {
    let name: String = type_url
        .strip_prefix(TYPE_URL_PREFIX)
        .unwrap_or(type_url)
        .chars()
        .take(64)
        .collect();
    Status::unimplemented(format!("flight sql: {name} is not supported"))
}

fn client_error(e: &dyn std::fmt::Display) -> Status {
    Status::invalid_argument(format!("flight sql: {e}"))
}

fn internal(e: &dyn std::fmt::Display) -> Status {
    Status::internal(format!("flight sql: {e}"))
}

/// Statement tickets and prepared-statement handles are the SQL text.
fn sql_of_handle(handle: &[u8]) -> Result<&str, Status> {
    std::str::from_utf8(handle)
        .map_err(|_| Status::invalid_argument("flight sql: unknown statement handle"))
}

fn statement_ticket(sql: &str) -> Ticket {
    let ticket = TicketStatementQuery {
        statement_handle: sql.as_bytes().to_vec().into(),
    };
    Ticket::new(ticket.as_any().encode_to_vec())
}

/// There are no transactions to run a statement in. An empty id is what some clients send for
/// "none".
fn no_transaction(id: Option<&[u8]>) -> Result<(), Status> {
    if id.is_some_and(|id| !id.is_empty()) {
        return Err(Status::unimplemented(
            "flight sql: transactions are not supported (the server is read-only)",
        ));
    }
    Ok(())
}

/// The body of a Flight SQL action: an `Any` holding an `M`.
fn unpack<M: ProstMessageExt>(body: &[u8]) -> Result<M, Status> {
    let any = Any::decode(body)
        .map_err(|e| Status::invalid_argument(format!("flight sql: malformed action body: {e}")))?;
    any.unpack::<M>()
        .map_err(|e| Status::invalid_argument(format!("flight sql: malformed action body: {e}")))?
        .ok_or_else(|| Status::invalid_argument(format!("flight sql: expected {}", M::type_url())))
}

fn table_type_name(t: TableType) -> &'static str {
    match t {
        TableType::Base => "TABLE",
        TableType::View => "VIEW",
        TableType::Temporary => "LOCAL TEMPORARY",
    }
}

/// A finished batch as a Flight stream (schema message, then the data).
fn one_batch(batch: RecordBatch) -> BoxStream<FlightData> {
    let flight = FlightDataEncoderBuilder::new()
        .with_schema(batch.schema())
        .build(futures::stream::iter([Ok(batch)]))
        .map_err(|e| match e {
            arrow_flight::error::FlightError::Tonic(s) => *s,
            e => Status::internal(e.to_string()),
        });
    Box::pin(flight)
}

/// Result schema of a metadata command (`None` for any other command).
fn metadata_schema(cmd: &Command) -> Option<SchemaRef> {
    match cmd {
        Command::CommandGetCatalogs(c) => Some(c.into_builder().schema()),
        Command::CommandGetDbSchemas(c) => Some(c.clone().into_builder().schema()),
        Command::CommandGetTables(c) => Some(c.clone().into_builder().schema()),
        Command::CommandGetTableTypes(c) => Some(c.into_builder().schema()),
        Command::CommandGetSqlInfo(_) => Some(Arc::new(SqlInfoDataBuilder::schema().clone())),
        _ => None,
    }
}

impl AnkaaFlight {
    /// `GetFlightInfo` for a Flight SQL command: the result schema and one endpoint on this
    /// server.
    pub(crate) async fn sql_flight_info(
        &self,
        any: Any,
        descriptor: FlightDescriptor,
    ) -> Result<FlightInfo, Status> {
        let (schema, ticket) = self.sql_describe(any).await?;
        Ok(FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|e| internal(&e))?
            .with_descriptor(descriptor)
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket)))
    }

    /// `GetSchema` for a Flight SQL command.
    pub(crate) async fn sql_schema(&self, any: Any) -> Result<SchemaRef, Status> {
        Ok(self.sql_describe(any).await?.0)
    }

    /// Result schema of a command and the ticket that fetches the result.
    async fn sql_describe(&self, any: Any) -> Result<(SchemaRef, Ticket), Status> {
        match command(any)? {
            Command::CommandStatementQuery(q) => {
                no_transaction(q.transaction_id.as_deref())?;
                let schema = self.statement_schema(&q.query).await?;
                Ok((schema, statement_ticket(&q.query)))
            }
            Command::CommandPreparedStatementQuery(p) => {
                let sql = sql_of_handle(&p.prepared_statement_handle)?;
                let schema = self.statement_schema(sql).await?;
                Ok((schema, statement_ticket(sql)))
            }
            cmd => {
                let Some(schema) = metadata_schema(&cmd) else {
                    return Err(unsupported(cmd.type_url()));
                };
                // the command is its own ticket
                Ok((schema, Ticket::new(cmd.into_any().encode_to_vec())))
            }
        }
    }

    /// Plan a statement (read-only, inside the query timeout) and return its result schema.
    async fn statement_schema(&self, sql: &str) -> Result<SchemaRef, Status> {
        let df = self.plan(sql).await?;
        let parameters = df
            .logical_plan()
            .get_parameter_names()
            .map_err(|e| Status::invalid_argument(format!("sql: {e}")))?;
        if !parameters.is_empty() {
            return Err(no_parameters());
        }
        Ok(crate::wire_schema(df.schema().as_arrow()))
    }

    /// `DoGet` for a Flight SQL ticket.
    pub(crate) async fn sql_do_get(&self, any: Any) -> Result<BoxStream<FlightData>, Status> {
        match command(any)? {
            Command::TicketStatementQuery(t) => {
                self.run_query(sql_of_handle(&t.statement_handle)?).await
            }
            Command::CommandPreparedStatementQuery(p) => {
                self.run_query(sql_of_handle(&p.prepared_statement_handle)?)
                    .await
            }
            cmd => {
                let batch = tokio::time::timeout(self.query_timeout, self.metadata(cmd))
                    .await
                    .map_err(|_| {
                        Status::deadline_exceeded(
                            "metadata request exceeded the server's query timeout",
                        )
                    })??;
                Ok(one_batch(batch))
            }
        }
    }

    /// `DoAction`: the two prepared-statement actions; the rest of Flight SQL is refused by name.
    pub(crate) async fn sql_do_action(
        &self,
        action: Action,
    ) -> Result<BoxStream<arrow_flight::Result>, Status> {
        match action.r#type.as_str() {
            CREATE_PREPARED_STATEMENT => {
                let req: ActionCreatePreparedStatementRequest = unpack(&action.body)?;
                no_transaction(req.transaction_id.as_deref())?;
                let schema = self.statement_schema(&req.query).await?;
                let options = arrow::ipc::writer::IpcWriteOptions::default();
                let IpcMessage(dataset_schema) = SchemaAsIpc::new(&schema, &options)
                    .try_into()
                    .map_err(|e: ArrowError| internal(&e))?;
                let result = ActionCreatePreparedStatementResult {
                    prepared_statement_handle: req.query.into_bytes().into(),
                    dataset_schema,
                    // no parameters: an empty schema is sent as no bytes
                    parameter_schema: Vec::new().into(),
                };
                let body = arrow_flight::Result {
                    body: result.as_any().encode_to_vec().into(),
                };
                Ok(Box::pin(futures::stream::iter([Ok(body)])))
            }
            CLOSE_PREPARED_STATEMENT => {
                // nothing is held for a handle; the request is still checked
                let _: ActionClosePreparedStatementRequest = unpack(&action.body)?;
                Ok(Box::pin(futures::stream::empty()))
            }
            other => Err(REFUSED_ACTIONS
                .iter()
                .find(|(name, _)| *name == other)
                .map_or_else(
                    || Status::unimplemented("do_action"),
                    |(name, why)| Status::unimplemented(format!("flight sql: {name}: {why}")),
                )),
        }
    }

    /// The result of a metadata command, filtered and ordered as the specification asks.
    async fn metadata(&self, cmd: Command) -> Result<RecordBatch, Status> {
        match cmd {
            Command::CommandGetCatalogs(c) => {
                let mut b = c.into_builder();
                for catalog in self.ctx.catalog_names() {
                    b.append(catalog);
                }
                b.build().map_err(|e| internal(&e))
            }
            Command::CommandGetDbSchemas(c) => {
                let mut b = c.into_builder();
                for catalog_name in self.ctx.catalog_names() {
                    let Some(catalog) = self.ctx.catalog(&catalog_name) else {
                        continue;
                    };
                    for schema_name in catalog.schema_names() {
                        b.append(&catalog_name, schema_name);
                    }
                }
                // a failure here comes from the client's filter pattern
                b.build().map_err(|e| client_error(&e))
            }
            Command::CommandGetTables(c) => {
                let mut b = c.into_builder();
                for catalog_name in self.ctx.catalog_names() {
                    let Some(catalog) = self.ctx.catalog(&catalog_name) else {
                        continue;
                    };
                    for schema_name in catalog.schema_names() {
                        let Some(schema) = catalog.schema(&schema_name) else {
                            continue;
                        };
                        for table_name in schema.table_names() {
                            // a table dropped since the listing is skipped
                            let Ok(Some(table)) = schema.table(&table_name).await else {
                                continue;
                            };
                            b.append(
                                &catalog_name,
                                &schema_name,
                                &table_name,
                                table_type_name(table.table_type()),
                                crate::wire_schema(table.schema().as_ref()).as_ref(),
                            )
                            .map_err(|e| internal(&e))?;
                        }
                    }
                }
                b.build().map_err(|e| client_error(&e))
            }
            Command::CommandGetTableTypes(c) => {
                let mut b = c.into_builder();
                for t in [TableType::Base, TableType::View, TableType::Temporary] {
                    b.append(table_type_name(t));
                }
                b.build().map_err(|e| internal(&e))
            }
            Command::CommandGetSqlInfo(c) => Self::sql_info(c),
            other => Err(unsupported(other.type_url())),
        }
    }

    /// `CommandGetSqlInfo`: what this server is and what it does not do. Ids the server has no
    /// value for are left out of the answer.
    fn sql_info(cmd: CommandGetSqlInfo) -> Result<RecordBatch, Status> {
        let mut b = SqlInfoDataBuilder::new();
        b.append(SqlInfo::FlightSqlServerName, "ANKAA");
        b.append(SqlInfo::FlightSqlServerVersion, crate::VERSION);
        b.append(SqlInfo::FlightSqlServerArrowVersion, arrow::ARROW_VERSION);
        b.append(SqlInfo::FlightSqlServerReadOnly, true);
        b.append(SqlInfo::FlightSqlServerSql, true);
        b.append(SqlInfo::FlightSqlServerSubstrait, false);
        b.append(
            SqlInfo::FlightSqlServerTransaction,
            SqlSupportedTransaction::None as i32,
        );
        b.append(SqlInfo::FlightSqlServerCancel, false);
        b.append(SqlInfo::FlightSqlServerBulkIngestion, false);
        // prepared-statement handles do not expire (nothing is kept for them)
        b.append(SqlInfo::FlightSqlServerStatementTimeout, 0i32);
        b.append(SqlInfo::SqlDdlCatalog, false);
        b.append(SqlInfo::SqlDdlSchema, false);
        b.append(SqlInfo::SqlDdlTable, false);
        b.append(
            SqlInfo::SqlIdentifierCase,
            SqlSupportedCaseSensitivity::SqlCaseSensitivityLowercase as i32,
        );
        b.append(SqlInfo::SqlIdentifierQuoteChar, "\"");
        b.append(SqlInfo::SqlAllTablesAreSelectable, true);
        b.append(SqlInfo::SqlTransactionsSupported, false);
        let data = b.build().map_err(|e| internal(&e))?;
        let all = data
            .record_batch(std::iter::empty())
            .map_err(|e| internal(&e))?;
        if cmd.info.is_empty() {
            return Ok(all);
        }
        // the requested ids come from the client: each distinct known one is looked up once
        let known: BTreeSet<u32> = all
            .columns()
            .first()
            .and_then(|names| names.as_primitive_opt::<UInt32Type>())
            .map(|ids| ids.values().iter().copied().collect())
            .unwrap_or_default();
        let wanted: BTreeSet<u32> = cmd
            .info
            .into_iter()
            .filter(|id| known.contains(id))
            .collect();
        if wanted.is_empty() {
            return Ok(all.slice(0, 0));
        }
        data.record_batch(wanted).map_err(|e| internal(&e))
    }
}
