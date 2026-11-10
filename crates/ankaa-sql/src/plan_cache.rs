//! Reuse of logical plans for statements that are sent again.
//!
//! Parsing, analysing and optimising a statement costs a few tenths of a millisecond, which is
//! most of the time of a small query (a time-range aggregate executes in tens of microseconds).
//! Dashboards and services send the same text over and over. [`PlanCache`] keeps the optimised
//! logical plan of a statement and, on a repeat, only builds the physical plan: that step is
//! never cached, because it binds the scan to the chunks that exist at that moment.
//!
//! A plan is kept only when using it again cannot change an answer:
//!
//! - it is a query (a statement such as `SET` is never cached and empties the cache: it may
//!   change how queries are planned);
//! - it calls no function whose value depends on when it is evaluated (`now()`,
//!   `current_date`, `random()`…): the optimiser folds those into constants;
//! - the catalog is what it was: before every use the tables registered in the session are
//!   compared with those the cache was filled under (names and provider identities), and any
//!   difference empties the cache. Tables that are updated in place (chunk tables, published
//!   batch tables) keep their identity, so live data does not invalidate anything.

use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::context::SQLOptions;
use datafusion::logical_expr::{Expr, LogicalPlan, Volatility};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use parking_lot::Mutex;

/// Statements kept at most.
const CAPACITY: usize = 256;
/// Longest statement text that is kept.
const MAX_SQL_BYTES: usize = 16 * 1024;

#[derive(Default)]
struct Inner {
    catalog: u64,
    plans: HashMap<String, Arc<LogicalPlan>>,
    order: VecDeque<String>,
}

/// See the module documentation.
#[derive(Default)]
pub struct PlanCache {
    inner: Mutex<Inner>,
    hits: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for PlanCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlanCache")
            .field("plans", &self.inner.lock().plans.len())
            .finish_non_exhaustive()
    }
}

impl PlanCache {
    /// Statements answered from the cache so far.
    #[must_use]
    pub fn hits(&self) -> u64 {
        self.hits.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The physical plan of `sql` for the session's data as it is now, checked against
    /// `options` (a statement the options forbid is refused whether or not it was seen before:
    /// only statements that passed are ever kept).
    pub async fn physical_plan(
        &self,
        ctx: &SessionContext,
        sql: &str,
        options: SQLOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let state = ctx.state();
        let catalog = catalog_identity(ctx).await;
        let cached = {
            let mut inner = self.inner.lock();
            if inner.catalog != catalog {
                inner.plans.clear();
                inner.order.clear();
                inner.catalog = catalog;
            }
            inner.plans.get(sql).cloned()
        };
        if let Some(plan) = cached {
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return state
                .query_planner()
                .create_physical_plan(&plan, &state)
                .await;
        }
        let plan = state.create_logical_plan(sql).await?;
        options.verify_plan(&plan)?;
        if !is_query(&plan) {
            // a statement (SET, DDL, EXPLAIN…) is run the ordinary way; it may change how
            // later queries are planned, so nothing planned before it is kept
            {
                let mut inner = self.inner.lock();
                inner.plans.clear();
                inner.order.clear();
            }
            return ctx
                .execute_logical_plan(plan)
                .await?
                .create_physical_plan()
                .await;
        }
        let reusable = is_time_independent(&plan)?;
        let optimized = Arc::new(state.optimize(&plan)?);
        {
            let mut inner = self.inner.lock();
            if reusable && sql.len() <= MAX_SQL_BYTES && inner.catalog == catalog {
                if inner.order.len() == CAPACITY
                    && let Some(oldest) = inner.order.pop_front()
                {
                    inner.plans.remove(&oldest);
                }
                if inner
                    .plans
                    .insert(sql.to_owned(), optimized.clone())
                    .is_none()
                {
                    inner.order.push_back(sql.to_owned());
                }
            }
        }
        state
            .query_planner()
            .create_physical_plan(&optimized, &state)
            .await
    }
}

/// A plan that only reads.
fn is_query(plan: &LogicalPlan) -> bool {
    !matches!(
        plan,
        LogicalPlan::Statement(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::DescribeTable(_)
    )
}

/// True when no expression of the plan (subqueries included) calls a function that is not
/// immutable.
fn is_time_independent(plan: &LogicalPlan) -> Result<bool> {
    let mut pure = true;
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|e| {
                if let Expr::ScalarFunction(f) = e
                    && f.func.signature().volatility != Volatility::Immutable
                {
                    pure = false;
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })
    })?;
    Ok(pure)
}

/// A number that changes when a table is registered, removed or replaced by another provider.
async fn catalog_identity(ctx: &SessionContext) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    let mut catalogs = ctx.catalog_names();
    catalogs.sort_unstable();
    for c in catalogs {
        let Some(catalog) = ctx.catalog(&c) else {
            continue;
        };
        let mut schemas = catalog.schema_names();
        schemas.sort_unstable();
        for s in schemas {
            let Some(schema) = catalog.schema(&s) else {
                continue;
            };
            let mut tables = schema.table_names();
            tables.sort_unstable();
            for t in tables {
                (&c, &s, &t).hash(&mut h);
                if let Ok(Some(provider)) = schema.table(&t).await {
                    (Arc::as_ptr(&provider).cast::<()>() as usize).hash(&mut h);
                }
            }
        }
    }
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::register_chunks;
    use ankaa_store::{Chunk, ChunkId, Schema, Value};
    use datafusion::physical_plan::collect;

    fn chunk(rows: i64) -> Chunk {
        let mut c = Chunk::new(ChunkId(1), Schema::trades());
        for i in 0..rows {
            c.append_row(&[
                Value::I64(i),
                Value::U32(0),
                Value::I64(100 + i),
                Value::I64(1),
                Value::U8(0),
                Value::I64(i),
            ])
            .unwrap();
        }
        c.seal();
        c
    }

    async fn count(cache: &PlanCache, ctx: &SessionContext, sql: &str) -> Result<i64> {
        use arrow::array::AsArray;
        let plan = cache.physical_plan(ctx, sql, SQLOptions::new()).await?;
        let b = collect(plan, ctx.task_ctx()).await?;
        Ok(b[0]
            .column(0)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(0))
    }

    #[tokio::test]
    async fn repeats_are_reused_and_nothing_stale_is_served() {
        let ctx = crate::default_session();
        let table = Arc::new(crate::ChunkTable::new(Schema::trades(), vec![chunk(10)]));
        ctx.register_table("trades", table.clone()).unwrap();
        let cache = PlanCache::default();
        let q = "SELECT count(*) FROM trades WHERE price >= 100";
        assert_eq!(count(&cache, &ctx, q).await.unwrap(), 10);
        assert_eq!(cache.hits(), 0);
        assert_eq!(count(&cache, &ctx, q).await.unwrap(), 10);
        assert_eq!(cache.hits(), 1);
        // live data: the same table object with new chunks; the cached plan sees them
        table.swap(vec![ankaa_store::ChunkRef::Hot(Arc::new(chunk(25)))]);
        assert_eq!(count(&cache, &ctx, q).await.unwrap(), 25);
        assert_eq!(cache.hits(), 2);
        // another table under the same name: the cache lets go of everything
        let _ = ctx.deregister_table("trades").unwrap();
        register_chunks(&ctx, "trades", Schema::trades(), vec![chunk(3)]).unwrap();
        assert_eq!(count(&cache, &ctx, q).await.unwrap(), 3);
        assert_eq!(cache.hits(), 2);
        assert_eq!(count(&cache, &ctx, q).await.unwrap(), 3);
        assert_eq!(cache.hits(), 3);
        // a function of the clock is planned every time
        let clock = "SELECT count(*) FROM trades WHERE ts < now()";
        assert_eq!(count(&cache, &ctx, clock).await.unwrap(), 3);
        assert_eq!(count(&cache, &ctx, clock).await.unwrap(), 3);
        assert_eq!(cache.hits(), 3);
        // a statement the caller forbids is refused, seen before or not
        let read_only = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        for _ in 0..2 {
            assert!(
                cache
                    .physical_plan(&ctx, "CREATE TABLE x AS SELECT 1", read_only)
                    .await
                    .is_err()
            );
        }
        // SET may change planning: it empties the cache
        assert_eq!(count(&cache, &ctx, q).await.unwrap(), 3);
        let hits = cache.hits();
        let set = cache
            .physical_plan(
                &ctx,
                "SET datafusion.execution.batch_size = 1024",
                SQLOptions::new(),
            )
            .await
            .unwrap();
        collect(set, ctx.task_ctx()).await.unwrap();
        assert_eq!(count(&cache, &ctx, q).await.unwrap(), 3);
        assert_eq!(cache.hits(), hits);
    }
}
