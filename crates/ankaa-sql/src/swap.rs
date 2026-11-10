//! Small tables that are replaced whole and often (the symbol and dictionary tables, the live
//! rows of continuous queries). Replacing a registered table by "deregister, then register"
//! leaves a moment in which a concurrent query does not find it; a [`SwapTable`] stays
//! registered and exchanges its contents atomically instead.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::datasource::{MemTable, TableType};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

/// A registered table whose single batch can be exchanged while queries run.
pub struct SwapTable {
    schema: SchemaRef,
    inner: parking_lot::RwLock<Arc<MemTable>>,
}

impl std::fmt::Debug for SwapTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwapTable").finish_non_exhaustive()
    }
}

impl SwapTable {
    /// Table holding `batch`.
    pub fn new(batch: RecordBatch) -> Result<Self> {
        let schema = batch.schema();
        Ok(Self {
            inner: parking_lot::RwLock::new(Arc::new(MemTable::try_new(
                schema.clone(),
                vec![vec![batch]],
            )?)),
            schema,
        })
    }

    /// Replace the contents. The schema must be the one the table was created with.
    pub fn set(&self, batch: RecordBatch) -> Result<()> {
        let table = Arc::new(MemTable::try_new(self.schema.clone(), vec![vec![batch]])?);
        *self.inner.write() = table;
        Ok(())
    }
}

#[async_trait::async_trait]
impl TableProvider for SwapTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // a scan keeps the contents it started with
        let table = Arc::clone(&self.inner.read());
        table.scan(state, projection, filters, limit).await
    }
}

/// The swap tables one owner publishes into a session, by name.
#[derive(Debug, Default)]
pub struct BatchTables {
    tables: HashMap<String, Arc<SwapTable>>,
}

impl BatchTables {
    /// Make `batch` the contents of table `name`: exchanged in place when the table exists
    /// with the same schema, registered (replacing any other table of that name) otherwise.
    pub fn publish(&mut self, ctx: &SessionContext, name: &str, batch: RecordBatch) -> Result<()> {
        if let Some(t) = self.tables.get(name)
            && t.schema.fields() == batch.schema().fields()
        {
            return t.set(batch);
        }
        let table = Arc::new(SwapTable::new(batch)?);
        let _ = ctx.deregister_table(name)?;
        ctx.register_table(name, table.clone())?;
        self.tables.insert(name.to_owned(), table);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    #[tokio::test]
    async fn contents_are_exchanged_without_leaving_the_catalog() {
        let ctx = SessionContext::new();
        let mut tables = BatchTables::default();
        tables.publish(&ctx, "t", batch(vec![1, 2])).unwrap();
        let count = |ctx: SessionContext| async move {
            let b = ctx
                .sql("SELECT count(*) FROM t")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            b[0].column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(count(ctx.clone()).await, 2);
        // a plan made before the exchange still runs; a new one sees the new contents
        let df = ctx.sql("SELECT count(*) FROM t").await.unwrap();
        tables.publish(&ctx, "t", batch(vec![1, 2, 3])).unwrap();
        assert_eq!(df.collect().await.unwrap()[0].num_rows(), 1);
        assert_eq!(count(ctx.clone()).await, 3);
        // many readers while the owner keeps publishing: the table is always there
        let reader = {
            let ctx = ctx.clone();
            tokio::spawn(async move {
                for _ in 0..200 {
                    ctx.sql("SELECT count(*) FROM t")
                        .await
                        .expect("table present");
                }
            })
        };
        for i in 0..200 {
            tables.publish(&ctx, "t", batch(vec![i])).unwrap();
            tokio::task::yield_now().await;
        }
        reader.await.unwrap();
    }
}
