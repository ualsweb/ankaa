//! Names of a dictionary, published to queries (RFC-0002).
//!
//! A `dict` column stores ids; SQL shows it as a dictionary-encoded string
//! (`Dictionary(UInt32, Utf8)`): the ids of a chunk are the keys, used in place, and the values
//! are one array per dictionary shared by every batch of every table that uses it. Sharing one
//! values array matters: Arrow and `DataFusion` then never merge or re-encode dictionaries when
//! they put batches together, and grouping by the column can work on the ids.
//!
//! The owner of the dictionary (the ingest loop) publishes its names before it shows new rows
//! to queries, so every id a query can see has its name. Publishing appends: ids never change.
//! The lookup from a name to its id (for `WHERE host = 'web-1'`) grows by the new names only.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, DictionaryArray, RecordBatch, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, UInt32Type};
use parking_lot::RwLock;

/// Arrow type of a `dict` column whose names are published.
#[must_use]
pub fn dict_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8))
}

/// True for [`dict_type`] (and any other `UInt32`-keyed dictionary).
#[must_use]
pub fn is_dict(t: &DataType) -> bool {
    matches!(t, DataType::Dictionary(k, _) if **k == DataType::UInt32)
}

#[derive(Debug)]
struct Published {
    values: ArrayRef,
    index: HashMap<Box<str>, u32>,
}

/// The published names of one dictionary: id `i` is `values[i]`.
#[derive(Debug)]
pub struct Names {
    state: RwLock<Published>,
}

impl Default for Names {
    fn default() -> Self {
        Self {
            state: RwLock::new(Published {
                values: Arc::new(StringArray::from(Vec::<&str>::new())),
                index: HashMap::new(),
            }),
        }
    }
}

impl Names {
    /// An empty dictionary, ready to be shared.
    #[must_use]
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// A dictionary holding `names` (id = position).
    #[must_use]
    pub fn of<S: AsRef<str>>(names: &[S]) -> Arc<Self> {
        let n = Self::shared();
        n.publish(names);
        n
    }

    /// Publish `names` (all of them, in id order; the first [`Self::len`] are the ones already
    /// published). Returns whether anything was added. The values array is rebuilt only when
    /// names were added; the name → id lookup grows by the new names only.
    pub fn publish<S: AsRef<str>>(&self, names: &[S]) -> bool {
        let known = self.state.read().values.len();
        if names.len() <= known {
            return false;
        }
        let values: ArrayRef = Arc::new(StringArray::from_iter_values(
            names.iter().map(AsRef::as_ref),
        ));
        let mut s = self.state.write();
        for (id, name) in names.iter().enumerate().skip(s.values.len()) {
            if let Ok(id) = u32::try_from(id) {
                s.index.entry(name.as_ref().into()).or_insert(id);
            }
        }
        s.values = values;
        true
    }

    /// The names as an Arrow array (id = position); a reference, not a copy.
    #[must_use]
    pub fn values(&self) -> ArrayRef {
        self.state.read().values.clone()
    }

    /// The id of `name`, when it has one.
    #[must_use]
    pub fn id_of(&self, name: &str) -> Option<u32> {
        self.state.read().index.get(name).copied()
    }

    /// Number of published names.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state.read().values.len()
    }

    /// Nothing published.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// `batch` with the id columns that have names (`names[i]` for column `i`; `None` or missing
/// leaves a column alone) turned into dictionary-encoded strings: for small result batches
/// built outside a chunk scan (live rows of continuous queries, the symbol list).
pub fn named_batch(
    batch: &RecordBatch,
    names: &[Option<Arc<Names>>],
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let schema = batch.schema();
    let mut fields: Vec<Field> = Vec::with_capacity(schema.fields().len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (i, (f, col)) in schema.fields().iter().zip(batch.columns()).enumerate() {
        let ids = col.as_any().downcast_ref::<UInt32Array>();
        if let (Some(n), Some(ids)) = (names.get(i).and_then(Option::as_ref), ids) {
            fields.push(Field::new(f.name(), dict_type(), f.is_nullable()));
            columns.push(Arc::new(DictionaryArray::<UInt32Type>::try_new(
                ids.clone(),
                n.values(),
            )?));
        } else {
            fields.push(f.as_ref().clone());
            columns.push(col.clone());
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publishing_appends_and_keeps_ids() {
        let n = Names::shared();
        assert!(n.is_empty() && n.id_of("a").is_none());
        assert!(n.publish(&["a", "b"]));
        let first = n.values();
        assert_eq!((n.id_of("a"), n.id_of("b"), n.len()), (Some(0), Some(1), 2));
        // nothing new: the same array, no rebuild
        assert!(!n.publish(&["a", "b"]));
        assert!(Arc::ptr_eq(&first, &n.values()));
        assert!(n.publish(&["a", "b", "c"]));
        assert_eq!((n.id_of("a"), n.id_of("c"), n.len()), (Some(0), Some(2), 3));
        // a batch built on the earlier array stays valid: it is a reference of its own
        assert_eq!(first.len(), 2);
    }
}
