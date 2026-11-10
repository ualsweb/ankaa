//! Apache `DataFusion` integration: `TableProvider`, `ExecutionPlan`s, UDFs, pushdown.
//!
//! P0-05 scope: expose a set of in-memory [`ankaa_store::Chunk`]s as a `DataFusion` table with
//! **time-range and projection pushdown**: `WHERE ts BETWEEN a AND b` (and `>=`, `<=`, `>`, `<`
//! on the timestamp column) prunes whole chunks by zone map / watermark and slices the
//! surviving chunks by binary search; only the projected columns are converted to Arrow.
//!
//! Hot chunks are handed to Arrow in place: a batch points into the chunk's own vectors and
//! keeps the chunk alive, so a scan copies no column data (see [`arrow_conv`]); decoded columns
//! of a warm or cold chunk are moved in. A registered [`ChunkTable`] can follow a live store through
//! [`ChunkTable::swap`] without being re-registered.
#![forbid(unsafe_code)]

pub mod arrow_conv;
pub mod dense_agg;
pub mod eager_agg;
pub mod functions;
pub mod latest;
pub mod merge;
pub mod plan_cache;
pub mod provider;
pub mod swap;
pub mod vwap;

pub use dense_agg::{DenseAggExec, DenseAggregation};
pub use functions::{PERCENTILE_ALPHA, PERCENTILE_MAX_BUCKETS, register_functions};
pub use plan_cache::PlanCache;
pub use provider::{ChunkTable, register_chunks, register_snapshot};
pub use swap::{BatchTables, SwapTable};

/// A session with ANKAA's physical optimiser rules (hash-free aggregation over dense keys,
/// see [`dense_agg`]) on top of `DataFusion`'s own, unless the environment variable
/// `ANKAA_DENSE_AGGREGATION` is `off`. Table functions are registered separately with
/// [`register_functions`].
#[must_use]
pub fn session(
    config: datafusion::prelude::SessionConfig,
    runtime: std::sync::Arc<datafusion::execution::runtime_env::RuntimeEnv>,
) -> datafusion::prelude::SessionContext {
    let mut state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()
        .with_optimizer_rule(std::sync::Arc::new(eager_agg::EagerAggregation));
    // `ANKAA_DENSE_AGGREGATION=off` leaves every aggregate to the hash aggregate: a switch for
    // operators (the operator is new) and for measuring one against the other
    if std::env::var_os("ANKAA_DENSE_AGGREGATION").is_none_or(|v| v != "off") {
        state = state.with_physical_optimizer_rule(std::sync::Arc::new(DenseAggregation));
    }
    state = state.with_physical_optimizer_rule(std::sync::Arc::new(latest::LatestRows));
    state = state.with_physical_optimizer_rule(std::sync::Arc::new(merge::OrderedMerge));
    datafusion::prelude::SessionContext::new_with_state(state.build())
}

/// [`session`] with default configuration and runtime.
#[must_use]
pub fn default_session() -> datafusion::prelude::SessionContext {
    session(
        datafusion::prelude::SessionConfig::new(),
        std::sync::Arc::new(datafusion::execution::runtime_env::RuntimeEnv::default()),
    )
}

/// Crate version string (from Cargo).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
