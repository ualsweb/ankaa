//! Table schema: ordered typed fields; the first field is always the `i64` nanosecond timestamp.

/// Physical column types supported by the hot store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ColumnType {
    /// Nanoseconds since the Unix epoch (UTC), `i64`.
    TimestampNs,
    /// Fixed-point decimal stored as `i64` ticks with `scale` decimal places.
    FixedPoint {
        /// Number of decimal places (price = ticks / 10^scale).
        scale: u8,
    },
    /// Plain `i64` (sizes, sequence numbers).
    Int64,
    /// Dictionary-encoded identifier (symbol, venue, side), `u32`.
    DictU32,
    /// Flags / enum byte.
    U8,
    /// Derived float feature (ALP-encoded when cold).
    Float64,
}

/// A named, typed column.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Field {
    /// Column name (unique within a schema).
    pub name: String,
    /// Physical type.
    pub ty: ColumnType,
    /// The column may hold `NULL` (a reading without this value). Never the timestamp.
    #[serde(default)]
    pub nullable: bool,
}

impl Field {
    /// A column that always holds a value.
    pub fn new(name: impl Into<String>, ty: ColumnType) -> Self {
        Self {
            name: name.into(),
            ty,
            nullable: false,
        }
    }

    /// A column that may hold `NULL`.
    pub fn nullable(name: impl Into<String>, ty: ColumnType) -> Self {
        Self {
            name: name.into(),
            ty,
            nullable: true,
        }
    }
}

/// Schema errors.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SchemaError {
    /// The first field must be the timestamp.
    #[error("first field must be TimestampNs, found {0:?}")]
    FirstFieldNotTimestamp(ColumnType),
    /// Two fields share a name.
    #[error("duplicate field name {0:?}")]
    DuplicateField(String),
    /// A schema with no fields.
    #[error("schema has no fields")]
    Empty,
    /// The timestamp is never `NULL`.
    #[error("the timestamp field cannot be nullable")]
    NullableTimestamp,
}

/// Ordered list of fields; field 0 is the timestamp.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "Vec<Field>", into = "Vec<Field>")]
pub struct Schema {
    // shared: every partition and every chunk carries the schema, and a table with one
    // partition per device has hundreds of thousands of them; a clone is a reference count
    fields: std::sync::Arc<[Field]>,
}

impl TryFrom<Vec<Field>> for Schema {
    type Error = SchemaError;
    fn try_from(fields: Vec<Field>) -> Result<Self, SchemaError> {
        Self::new(fields)
    }
}

impl From<Schema> for Vec<Field> {
    fn from(s: Schema) -> Self {
        s.fields.to_vec()
    }
}

impl Schema {
    /// Validate and build a schema.
    pub fn new(fields: Vec<Field>) -> Result<Self, SchemaError> {
        let Some(first) = fields.first() else {
            return Err(SchemaError::Empty);
        };
        if first.ty != ColumnType::TimestampNs {
            return Err(SchemaError::FirstFieldNotTimestamp(first.ty));
        }
        if first.nullable {
            return Err(SchemaError::NullableTimestamp);
        }
        for (i, f) in fields.iter().enumerate() {
            if fields[..i].iter().any(|g| g.name == f.name) {
                return Err(SchemaError::DuplicateField(f.name.clone()));
            }
        }
        Ok(Self {
            fields: fields.into(),
        })
    }

    /// Fields in order.
    #[must_use]
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// Index of a field by name.
    #[must_use]
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }

    /// Number of fields.
    #[must_use]
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Always false for a valid schema (kept for API symmetry).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// The canonical trades schema used in examples and tests:
    /// `ts, symbol(dict), price(fixed 8), size(fixed 8), side(u8), seq(i64)`.
    #[must_use]
    pub fn trades() -> Self {
        Self::new(vec![
            Field::new("ts", ColumnType::TimestampNs),
            Field::new("symbol", ColumnType::DictU32),
            Field::new("price", ColumnType::FixedPoint { scale: 8 }),
            Field::new("size", ColumnType::FixedPoint { scale: 8 }),
            Field::new("side", ColumnType::U8),
            Field::new("seq", ColumnType::Int64),
        ])
        .expect("static schema is valid")
    }
}
