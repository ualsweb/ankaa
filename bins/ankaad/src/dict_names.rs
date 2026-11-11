//! Dictionary names as queries see them (RFC-0002): one published [`Names`] per dictionary
//! (the exchange symbols, and every `dict` column of every user table), handed to the query
//! tables that show their `dict` columns as strings. The ingest loop publishes them before it
//! swaps new rows into the query views, so every id a query can see has its name.

use std::sync::Arc;

use ankaa_ingest::SymbolDict;
use ankaa_sql::Names;
use ankaa_store::{ColumnType, Schema};

use crate::tables::Tables;

/// The market tables: their only `dict` column is the symbol.
const MARKET: [&str; 3] = ["trades", "book_deltas", "quotes"];

/// Every dictionary's published names.
pub struct DictNames {
    symbols: Arc<Names>,
    /// Per user table (same order as `Tables::user`), per column.
    user: Vec<Vec<Option<Arc<Names>>>>,
}

impl DictNames {
    /// One cell per dictionary of `tables` (the set of tables is fixed for a server's life).
    pub fn new(tables: &Tables) -> Self {
        Self {
            symbols: Names::shared(),
            user: tables
                .user
                .iter()
                .map(|u| {
                    u.dicts
                        .iter()
                        .map(|d| d.as_ref().map(|_| Names::shared()))
                        .collect()
                })
                .collect(),
        }
    }

    /// Publish the names added since the last call (cheap when nothing was added). Must run
    /// before the query views are swapped.
    pub fn publish(&self, symbols: &SymbolDict, tables: &Tables) {
        self.symbols.publish(symbols.names());
        for (cells, u) in self.user.iter().zip(&tables.user) {
            for (cell, dict) in cells.iter().zip(&u.dicts) {
                if let (Some(cell), Some(dict)) = (cell, dict) {
                    cell.publish(dict.names());
                }
            }
        }
    }

    /// The symbol names.
    pub fn symbols(&self) -> &Arc<Names> {
        &self.symbols
    }

    /// Names per column of a market table (every `dict` column is the symbol).
    pub fn market(&self, schema: &Schema) -> Vec<Option<Arc<Names>>> {
        schema
            .fields()
            .iter()
            .map(|f| (f.ty == ColumnType::DictU32).then(|| self.symbols.clone()))
            .collect()
    }

    /// Names per column of user table `index`.
    pub fn user(&self, index: usize) -> Vec<Option<Arc<Names>>> {
        self.user.get(index).cloned().unwrap_or_default()
    }

    /// Names of `column` of `table`.
    pub fn column(&self, tables: &Tables, table: &str, column: &str) -> Option<Arc<Names>> {
        if MARKET.contains(&table) {
            return (column == "symbol").then(|| self.symbols.clone());
        }
        let ti = tables.user.iter().position(|u| u.name == table)?;
        let ci = tables.user[ti].table.schema().index_of(column)?;
        self.user.get(ti)?.get(ci)?.clone()
    }

    /// Names per column of a table derived from `input` (a continuous query's output): a
    /// `dict` column that has the name of a `dict` column of the input uses its names; any
    /// other stays ids.
    pub fn derived(&self, tables: &Tables, input: &str, out: &Schema) -> Vec<Option<Arc<Names>>> {
        out.fields()
            .iter()
            .map(|f| {
                (f.ty == ColumnType::DictU32)
                    .then(|| self.column(tables, input, &f.name))
                    .flatten()
            })
            .collect()
    }
}
