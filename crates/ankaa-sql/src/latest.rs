//! "The latest N rows that match": `... WHERE <filter> ORDER BY ts [DESC] LIMIT n`.
//!
//! `DataFusion` hands an `ORDER BY ts` straight to a chunk scan (which is in time order, or
//! runs backwards for `DESC`) only when nothing lies between the sort and the scan. With a
//! filter in between it keeps a top-N sort that reads every row of the table. A filter does
//! not change the order of the rows it lets through, so [`LatestRows`] rewrites
//!
//! ```text
//! SortExec: TopK(fetch=n), expr=[ts DESC]        LocalLimitExec: fetch=n
//!   FilterExec: <filter>                    =>     FilterExec: <filter>
//!     [RepartitionExec: RoundRobin]                  ChunkScanExec reverse
//!       ChunkScanExec
//! ```
//!
//! and each partition stops as soon as it has `n` matching rows.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::replace_children_if_necessary;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::LocalLimitExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, Partitioning};

use crate::provider::ChunkScanExec;

/// See the module documentation.
#[derive(Debug, Default)]
pub struct LatestRows;

impl PhysicalOptimizerRule for LatestRows {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|node| {
            Ok(match rewrite(&node) {
                Some(new) => Transformed::yes(new),
                None => Transformed::no(node),
            })
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &'static str {
        "ankaa_latest_rows"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn rewrite(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let sort = plan.downcast_ref::<SortExec>()?;
    let fetch = sort.fetch()?;
    // one sorted stream per input partition is what the plan above expects; a sort that
    // merges several partitions itself is left alone
    let input = sort.input();
    if !sort.preserve_partitioning() && input.output_partitioning().partition_count() != 1 {
        return None;
    }
    let filter = input.downcast_ref::<FilterExec>()?;
    // a round-robin exchange under the filter only spreads batches over workers; with an early
    // stop there is nothing to spread, so the filter reads the scan directly
    let mut scan = filter.input();
    if let Some(r) = scan.downcast_ref::<RepartitionExec>()
        && matches!(r.partitioning(), Partitioning::RoundRobinBatch(_))
    {
        scan = r.input();
    }
    scan.downcast_ref::<ChunkScanExec>()?;
    // the sort names columns of the filter's output; the scan is asked in its own columns
    let request: Vec<PhysicalSortExpr> = sort
        .expr()
        .iter()
        .map(|key| {
            let col = key.expr.downcast_ref::<Column>()?;
            let below = match filter.projection() {
                Some(p) => *p.get(col.index())?,
                None => col.index(),
            };
            Some(PhysicalSortExpr::new(
                Arc::new(Column::new(scan.schema().field(below).name(), below)),
                key.options,
            ))
        })
        .collect::<Option<_>>()?;
    // the scan answers for the order (or declines): same request the planner would make
    let ordered = scan.try_pushdown_sort(&request).ok()?.into_inner()?;
    let filtered = replace_children_if_necessary(input.clone(), vec![ordered]).ok()?;
    Some(Arc::new(LocalLimitExec::new(filtered, fetch)))
}
