//! Generic ingest ("bring your own feed"): map JSON or CSV messages onto a user-defined
//! schema (`[[tables]]` + `[[sources]]` in the server config) and produce store rows.
//!
//! The mapper is allocation-light after warm-up: JSON is parsed once into a `serde_json::Value`
//! (sources are low-rate telemetry, not exchange feeds), decimals are parsed with
//! [`parse_fixed`] (no floats for `fixed8`), strings go through a per-column [`SymbolDict`].
#![allow(
    clippy::many_single_char_names,
    clippy::unnested_or_patterns,
    clippy::match_wildcard_for_single_variants,
    clippy::match_same_arms,
    clippy::large_digit_groups,
    clippy::inconsistent_digit_grouping,
    clippy::unreadable_literal
)]

use std::collections::HashMap;

use ankaa_store::{ColumnType, Field, Schema, Value};

use crate::symbols::{SymbolDict, SymbolId};
use crate::{DecodeError, parse_fixed};

/// Unit of an incoming timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TsUnit {
    /// Seconds, integer or decimal.
    Seconds,
    /// Milliseconds.
    Millis,
    /// Microseconds.
    Micros,
    /// Nanoseconds.
    #[default]
    Nanos,
    /// RFC 3339 text.
    Rfc3339,
}

/// Where a column's value comes from in a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selector {
    /// JSON path `a.b.c` (dots descend into objects, digits index arrays).
    Path(Vec<String>),
    /// CSV field index.
    Index(usize),
    /// The message topic (`$topic`) or one `/`-separated segment of it (`$topic.N`, from 0):
    /// MQTT puts the plant, line or device in the topic rather than in the payload.
    Topic(Option<usize>),
    /// Not mapped: timestamp from receive time, or a default/null.
    Absent,
}

/// Compiled mapping for one table.
#[derive(Debug)]
pub struct RowMapper {
    schema: Schema,
    selectors: Vec<Selector>,
    ts_unit: TsUnit,
    ts_from_receive: bool,
    /// Index of the partition column (`DictU32`), if any.
    partition_col: Option<usize>,
    /// Scratch row.
    row: Vec<Value>,
    /// Scratch CSV field ranges.
    csv_fields: Vec<(usize, usize)>,
    /// Cap on distinct values per dictionary column.
    max_dict_entries: usize,
    /// Dictionary values of the current row that are not interned yet (per column; the
    /// strings are reused between rows).
    pending: Vec<String>,
    /// Which entries of `pending` belong to the current row.
    pending_set: Vec<bool>,
    /// Messages arrive as `topic \0 payload` (MQTT sources).
    topic_prefixed: bool,
}

/// What [`RowMapper::parse`] found, before anything was added to a dictionary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parsed {
    /// Timestamp of the row, ns.
    pub ts: i64,
    /// Partition key when its value is already in the dictionary (0 for a table without a
    /// partition column); `None` when this row introduces a new key.
    pub key: Option<u32>,
}

/// Mapping error (message rejected, counted by the caller).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MapError {
    /// Message is not valid JSON / CSV.
    #[error("malformed message: {0}")]
    Malformed(&'static str),
    /// A mapped field is missing.
    #[error("missing field for column {0}")]
    Missing(String),
    /// A field has the wrong type or cannot be converted.
    #[error("column {column}: {reason}")]
    Convert {
        /// Column name.
        column: String,
        /// Why.
        reason: String,
    },
}

impl From<MapError> for DecodeError {
    fn from(e: MapError) -> Self {
        match e {
            MapError::Malformed(m) => Self::Malformed(m),
            MapError::Missing(_) => Self::Malformed("missing mapped field"),
            MapError::Convert { .. } => Self::Malformed("field conversion failed"),
        }
    }
}

impl RowMapper {
    /// Build a mapper. `map` is column name → JSON path or CSV index (as text).
    pub fn new(
        schema: Schema,
        map: &HashMap<String, String>,
        csv: bool,
        ts_unit: TsUnit,
        ts_from_receive: bool,
        partition_by: Option<&str>,
    ) -> Result<Self, MapError> {
        let mut selectors = Vec::with_capacity(schema.fields().len());
        for f in schema.fields() {
            let sel = match map.get(&f.name) {
                None => Selector::Absent,
                Some(spec) if spec.trim() == "$topic" => Selector::Topic(None),
                Some(spec) if spec.trim().starts_with("$topic.") => {
                    let n =
                        spec.trim()["$topic.".len()..]
                            .parse()
                            .map_err(|_| MapError::Convert {
                                column: f.name.clone(),
                                reason: format!(
                                    "{spec:?}: expected $topic or $topic.<segment number>"
                                ),
                            })?;
                    Selector::Topic(Some(n))
                }
                Some(spec) if csv => {
                    Selector::Index(spec.trim().parse().map_err(|_| MapError::Convert {
                        column: f.name.clone(),
                        reason: format!("CSV index {spec:?} is not a number"),
                    })?)
                }
                Some(spec) => Selector::Path(spec.split('.').map(str::to_owned).collect()),
            };
            selectors.push(sel);
        }
        let partition_col = match partition_by {
            Some(p) => Some(
                schema
                    .fields()
                    .iter()
                    .position(|f| f.name == p)
                    .ok_or_else(|| MapError::Missing(p.to_owned()))?,
            ),
            None => None,
        };
        let n = schema.fields().len();
        Ok(Self {
            schema,
            selectors,
            ts_unit,
            ts_from_receive,
            partition_col,
            row: vec![Value::I64(0); n],
            csv_fields: Vec::new(),
            max_dict_entries: DEFAULT_MAX_DICT_ENTRIES,
            pending: vec![String::new(); n],
            pending_set: vec![false; n],
            topic_prefixed: false,
        })
    }

    /// Messages carry their topic in front of the payload, separated by a NUL byte (an MQTT
    /// topic never contains one): `$topic` selectors read it.
    #[must_use]
    pub fn with_topic_prefix(mut self, on: bool) -> Self {
        self.topic_prefixed = on;
        self
    }

    /// Fresh dictionaries for a schema: one per `dict` column (owned by the table so that every
    /// source feeding it shares ids).
    #[must_use]
    pub fn dictionaries_for(schema: &Schema) -> Vec<Option<SymbolDict>> {
        schema
            .fields()
            .iter()
            .map(|f| (f.ty == ColumnType::DictU32).then(SymbolDict::new))
            .collect()
    }

    /// Table schema.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Partition key of the last mapped row (0 when the table has no partition column).
    #[must_use]
    pub fn partition_key(&self) -> u32 {
        match self.partition_col {
            Some(i) => match self.row[i] {
                Value::U32(k) => k,
                _ => 0,
            },
            None => 0,
        }
    }

    /// Cap the number of distinct values per dictionary column (default
    /// [`DEFAULT_MAX_DICT_ENTRIES`]); rows that would add a value beyond it are rejected.
    #[must_use]
    pub fn with_max_dict_entries(mut self, n: usize) -> Self {
        self.max_dict_entries = n.max(1);
        self
    }

    /// Map one message using the table's `dicts` (from [`Self::dictionaries_for`]). Returns the
    /// partition key and the row (valid until the next call). Nothing is added to a dictionary
    /// unless every column converted.
    pub fn map(
        &mut self,
        msg: &[u8],
        recv_ns: i64,
        csv: bool,
        dicts: &mut [Option<SymbolDict>],
    ) -> Result<(u32, &[Value]), MapError> {
        self.parse(msg, recv_ns, csv, dicts)?;
        Ok(self.finish(dicts))
    }

    /// Second half of [`Self::parse`]: add the row's new dictionary values and return the
    /// partition key and the row. Call it only once the row is going to be stored.
    pub fn finish(&mut self, dicts: &mut [Option<SymbolDict>]) -> (u32, &[Value]) {
        for i in 0..self.pending_set.len() {
            if std::mem::take(&mut self.pending_set[i])
                && let Some(d) = dicts.get_mut(i).and_then(Option::as_mut)
            {
                let SymbolId(id) = d.intern(&self.pending[i]);
                self.row[i] = Value::U32(id);
            }
        }
        (self.partition_key(), &self.row)
    }

    /// Convert one message into the scratch row without touching the dictionaries: a message
    /// that is rejected (here, or by the caller's own checks on the result) leaves no trace.
    /// New dictionary values are kept aside until [`Self::finish`].
    pub fn parse(
        &mut self,
        msg: &[u8],
        recv_ns: i64,
        csv: bool,
        dicts: &[Option<SymbolDict>],
    ) -> Result<Parsed, MapError> {
        self.pending_set.fill(false);
        let (topic, msg) = if self.topic_prefixed {
            let cut = msg
                .iter()
                .position(|&b| b == 0)
                .ok_or(MapError::Malformed("message without its topic"))?;
            let topic = std::str::from_utf8(&msg[..cut])
                .map_err(|_| MapError::Malformed("topic is not utf-8"))?;
            (topic, &msg[cut + 1..])
        } else {
            ("", msg)
        };
        let segment = |n: Option<usize>| match n {
            None => Some(topic),
            Some(n) => topic.split('/').nth(n),
        };
        if csv {
            let text =
                std::str::from_utf8(msg).map_err(|_| MapError::Malformed("csv is not utf-8"))?;
            let text = text.trim_end_matches(['\r', '\n']);
            split_csv(text, &mut self.csv_fields)?;
            for i in 0..self.schema.fields().len() {
                let raw = match &self.selectors[i] {
                    Selector::Index(k) => self.csv_fields.get(*k).map(|&(a, b)| text[a..b].trim()),
                    Selector::Topic(n) => segment(*n),
                    Selector::Path(_) | Selector::Absent => None,
                };
                self.set(i, raw.map(Scalar::Text), recv_ns, dicts)?;
            }
        } else {
            let v: serde_json::Value =
                serde_json::from_slice(msg).map_err(|_| MapError::Malformed("invalid json"))?;
            for i in 0..self.schema.fields().len() {
                let raw = match &self.selectors[i] {
                    Selector::Path(p) => lookup(&v, p).map(Scalar::Json),
                    Selector::Topic(n) => segment(*n).map(Scalar::Text),
                    Selector::Index(_) | Selector::Absent => None,
                };
                self.set(i, raw, recv_ns, dicts)?;
            }
        }
        let ts = match self.row[0] {
            Value::I64(t) => t,
            _ => 0,
        };
        let key = match self.partition_col {
            None => Some(0),
            Some(i) if self.pending_set[i] => None,
            Some(i) => match self.row[i] {
                Value::U32(k) => Some(k),
                _ => Some(0),
            },
        };
        Ok(Parsed { ts, key })
    }

    fn set(
        &mut self,
        i: usize,
        raw: Option<Scalar<'_>>,
        recv_ns: i64,
        dicts: &[Option<SymbolDict>],
    ) -> Result<(), MapError> {
        use serde_json::Value as J;
        let f: &Field = &self.schema.fields()[i];
        // the column name is only cloned on the error path
        let conv = |reason: &str| MapError::Convert {
            column: f.name.clone(),
            reason: reason.to_owned(),
        };
        // a nullable column without a value (field absent, JSON null, empty CSV field)
        let absent = match &raw {
            None => true,
            Some(Scalar::Json(serde_json::Value::Null)) => true,
            Some(Scalar::Text(s)) => s.is_empty(),
            Some(Scalar::Json(_)) => false,
        };
        if absent && f.nullable && i > 0 {
            self.row[i] = Value::Null;
            return Ok(());
        }
        let Some(raw) = raw else {
            if i == 0 && self.ts_from_receive {
                self.row[0] = Value::I64(recv_ns);
                return Ok(());
            }
            return Err(MapError::Missing(f.name.clone()));
        };
        let value = match f.ty {
            ColumnType::TimestampNs => {
                Value::I64(parse_ts(&raw, self.ts_unit).ok_or_else(|| conv("bad timestamp"))?)
            }
            ColumnType::FixedPoint { scale } => Value::I64(match &raw {
                Scalar::Text(s) => {
                    parse_fixed(s.as_bytes(), scale).map_err(|e| conv(&e.to_string()))?
                }
                Scalar::Json(J::String(s)) => {
                    parse_fixed(s.trim().as_bytes(), scale).map_err(|e| conv(&e.to_string()))?
                }
                Scalar::Json(J::Number(n)) => {
                    fixed_from_number(n, scale).ok_or_else(|| conv("number out of range"))?
                }
                Scalar::Json(_) => return Err(conv("expected a decimal")),
            }),
            ColumnType::Int64 => Value::I64(match &raw {
                Scalar::Text(s) => s.parse().map_err(|_| conv("expected an integer"))?,
                Scalar::Json(J::Number(n)) => {
                    n.as_i64().ok_or_else(|| conv("expected an integer"))?
                }
                Scalar::Json(J::String(s)) => {
                    s.trim().parse().map_err(|_| conv("expected an integer"))?
                }
                Scalar::Json(_) => return Err(conv("expected an integer")),
            }),
            ColumnType::U8 => Value::U8(match &raw {
                Scalar::Text(s) => s.parse().map_err(|_| conv("expected 0..255"))?,
                Scalar::Json(J::Number(n)) => {
                    u8::try_from(n.as_u64().unwrap_or(256)).map_err(|_| conv("expected 0..255"))?
                }
                Scalar::Json(J::Bool(b)) => u8::from(*b),
                Scalar::Json(_) => return Err(conv("expected 0..255")),
            }),
            ColumnType::Float64 => {
                let x: f64 = match &raw {
                    Scalar::Text(s) => s.parse().map_err(|_| conv("expected a number"))?,
                    Scalar::Json(J::Number(n)) => {
                        n.as_f64().ok_or_else(|| conv("expected a number"))?
                    }
                    Scalar::Json(J::String(s)) => {
                        s.trim().parse().map_err(|_| conv("expected a number"))?
                    }
                    Scalar::Json(_) => return Err(conv("expected a number")),
                };
                if !x.is_finite() {
                    return Err(conv("expected a finite number"));
                }
                Value::F64(x)
            }
            ColumnType::DictU32 => self.dict_value(i, &raw, dicts)?,
        };
        self.row[i] = value;
        Ok(())
    }

    /// The id of a `dict` value: known ids at once, a new value kept aside for `finish`.
    fn dict_value(
        &mut self,
        i: usize,
        raw: &Scalar<'_>,
        dicts: &[Option<SymbolDict>],
    ) -> Result<Value, MapError> {
        use serde_json::Value as J;
        let f: &Field = &self.schema.fields()[i];
        let conv = |reason: &str| MapError::Convert {
            column: f.name.clone(),
            reason: reason.to_owned(),
        };
        let mut buf = StackStr::default();
        let name: &str = match raw {
            Scalar::Text(s) => s,
            Scalar::Json(J::String(s)) => s.as_str(),
            Scalar::Json(J::Number(n)) => {
                use std::fmt::Write as _;
                write!(buf, "{n}").map_err(|_| conv("number too long"))?;
                buf.as_str()
            }
            Scalar::Json(J::Bool(true)) => "true",
            Scalar::Json(J::Bool(false)) => "false",
            // objects, arrays and null are not keys: interning their text would let one
            // producer grow the dictionary without bound
            Scalar::Json(_) => return Err(conv("expected a string or a number")),
        };
        if name.len() > MAX_DICT_VALUE_BYTES {
            return Err(conv("dictionary value longer than 256 bytes"));
        }
        let dict = dicts
            .get(i)
            .and_then(Option::as_ref)
            .ok_or_else(|| conv("not a dict column"))?;
        match dict.get(name) {
            Some(SymbolId(id)) => Ok(Value::U32(id)),
            None if dict.len() >= self.max_dict_entries => {
                Err(conv("dictionary is full (max_dict_entries)"))
            }
            None => {
                // interned by `finish`, once the whole row has been accepted
                self.pending[i].clear();
                self.pending[i].push_str(name);
                self.pending_set[i] = true;
                Ok(Value::U32(u32::MAX))
            }
        }
    }
}

/// Longest value accepted for a `dict` column, in bytes.
pub const MAX_DICT_VALUE_BYTES: usize = 256;

/// Default cap on distinct values per `dict` column (each value of a `partition_by` column is
/// one partition).
pub const DEFAULT_MAX_DICT_ENTRIES: usize = 1_000_000;

enum Scalar<'a> {
    Text(&'a str),
    Json(&'a serde_json::Value),
}

/// Small stack buffer for formatting a JSON number without allocating.
struct StackStr {
    buf: [u8; 40],
    len: usize,
}

impl Default for StackStr {
    fn default() -> Self {
        Self {
            buf: [0; 40],
            len: 0,
        }
    }
}

impl StackStr {
    fn as_str(&self) -> &str {
        // only `write_str` fills the buffer, with whole `&str`s
        std::str::from_utf8(&self.buf[..self.len]).unwrap_or("")
    }
}

impl std::fmt::Write for StackStr {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let end = self.len.checked_add(s.len()).ok_or(std::fmt::Error)?;
        self.buf
            .get_mut(self.len..end)
            .ok_or(std::fmt::Error)?
            .copy_from_slice(s.as_bytes());
        self.len = end;
        Ok(())
    }
}

/// A JSON number as a fixed-point integer with `scale` decimals. Exact when the number's text
/// has at most `scale` decimals; numbers with more decimals or an exponent (float artefacts
/// such as `21.700000000000003`, `1e-3`) are rounded to the nearest representable value.
fn fixed_from_number(n: &serde_json::Number, scale: u8) -> Option<i64> {
    use std::fmt::Write as _;
    if let Some(i) = n.as_i64() {
        return i.checked_mul(10i64.checked_pow(u32::from(scale))?);
    }
    if n.is_u64() {
        return None; // above i64::MAX
    }
    let mut buf = StackStr::default();
    if write!(buf, "{n}").is_ok()
        && let Ok(v) = parse_fixed(buf.as_str().as_bytes(), scale)
    {
        return Some(v);
    }
    let scaled = (n.as_f64()? * 10f64.powi(i32::from(scale))).round();
    // 2^63 is exactly representable; anything at or beyond it does not fit
    #[allow(clippy::cast_possible_truncation)]
    (scaled.is_finite() && scaled.abs() < 9_223_372_036_854_775_808.0).then_some(scaled as i64)
}

/// Split one CSV record into field byte ranges (no allocation after warm-up). A field may be
/// wrapped in double quotes, in which case it may contain commas; a doubled quote inside a
/// quoted field is kept as written.
fn split_csv(text: &str, out: &mut Vec<(usize, usize)>) -> Result<(), MapError> {
    out.clear();
    let b = text.as_bytes();
    let mut i = 0;
    loop {
        let mut start = i;
        while start < b.len() && b[start] == b' ' {
            start += 1;
        }
        if b.get(start) == Some(&b'"') {
            let mut j = start + 1;
            loop {
                match b.get(j) {
                    None => return Err(MapError::Malformed("csv: unterminated quote")),
                    Some(b'"') if b.get(j + 1) == Some(&b'"') => j += 2,
                    Some(b'"') => break,
                    Some(_) => j += 1,
                }
            }
            out.push((start + 1, j));
            j += 1;
            while j < b.len() && b[j] == b' ' {
                j += 1;
            }
            match b.get(j) {
                None => return Ok(()),
                Some(b',') => i = j + 1,
                Some(_) => return Err(MapError::Malformed("csv: text after a closing quote")),
            }
        } else {
            let end = b[i..]
                .iter()
                .position(|&c| c == b',')
                .map_or(b.len(), |p| i + p);
            out.push((i, end));
            if end == b.len() {
                return Ok(());
            }
            i = end + 1;
        }
    }
}

fn lookup<'a>(v: &'a serde_json::Value, path: &[String]) -> Option<&'a serde_json::Value> {
    let mut cur = v;
    for p in path {
        cur = match cur {
            serde_json::Value::Object(m) => m.get(p)?,
            serde_json::Value::Array(a) => a.get(p.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

fn parse_ts(raw: &Scalar<'_>, unit: TsUnit) -> Option<i64> {
    let scale = match unit {
        TsUnit::Nanos => 0,
        TsUnit::Micros => 3,
        TsUnit::Millis => 6,
        TsUnit::Seconds => 9,
        TsUnit::Rfc3339 => {
            return match raw {
                Scalar::Text(s) => parse_rfc3339_ns(s.trim()),
                Scalar::Json(serde_json::Value::String(s)) => parse_rfc3339_ns(s.trim()),
                Scalar::Json(_) => None,
            };
        }
    };
    match raw {
        Scalar::Text(s) => parse_fixed(s.trim().as_bytes(), scale).ok(),
        Scalar::Json(serde_json::Value::String(s)) => parse_fixed(s.trim().as_bytes(), scale).ok(),
        Scalar::Json(serde_json::Value::Number(n)) => fixed_from_number(n, scale),
        Scalar::Json(_) => None,
    }
}

/// RFC 3339 / ISO 8601 (`YYYY-MM-DDTHH:MM:SS[.fraction][Z|±HH:MM]`) → ns since the Unix epoch.
/// A missing offset means UTC. Anything after the offset, signs inside the fields and dates
/// that do not exist (`02-30`) are rejected.
#[must_use]
pub fn parse_rfc3339_ns(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 19
        || b[4] != b'-'
        || b[7] != b'-'
        || (b[10] != b'T' && b[10] != b't' && b[10] != b' ')
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    // digits only: `str::parse` would accept a sign
    let num = |r: std::ops::Range<usize>| -> Option<i64> {
        let d = b.get(r)?;
        if d.is_empty() || !d.iter().all(u8::is_ascii_digit) {
            return None;
        }
        Some(d.iter().fold(0i64, |a, &c| a * 10 + i64::from(c - b'0')))
    };
    let (y, mo, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (h, mi, sec) = (num(11..13)?, num(14..16)?, num(17..19)?);
    let mut i = 19;
    let mut frac_ns = 0i64;
    if b.get(i) == Some(&b'.') {
        i += 1;
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        let digits = i - start;
        if digits == 0 || digits > 9 {
            return None;
        }
        frac_ns = num(start..i)? * 10i64.pow(u32::try_from(9 - digits).ok()?);
    }
    let offset_s = match b.get(i) {
        None => 0,
        Some(b'Z' | b'z') if i + 1 == b.len() => 0,
        Some(sign @ (b'+' | b'-')) if i + 6 == b.len() && b[i + 3] == b':' => {
            let oh = num(i + 1..i + 3)?;
            let om = num(i + 4..i + 6)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let o = oh * 3600 + om * 60;
            if *sign == b'+' { -o } else { o }
        }
        _ => return None,
    };
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let days_in_month = match mo {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return None,
    };
    if !(1..=days_in_month).contains(&d) || h > 23 || mi > 59 || sec > 60 {
        return None;
    }
    // days from civil (Howard Hinnant's algorithm)
    let (y2, m2) = if mo <= 2 {
        (y - 1, mo + 9)
    } else {
        (y, mo - 3)
    };
    let era = y2.div_euclid(400);
    let yoe = y2 - era * 400;
    let doy = (153 * m2 + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = days * 86_400 + h * 3600 + mi * 60 + sec + offset_s;
    secs.checked_mul(1_000_000_000)?.checked_add(frac_ns)
}

/// Build a store schema from `(name, type)` pairs using the config type names.
pub fn schema_from(columns: &[(String, ColumnType)]) -> Result<Schema, ankaa_store::SchemaError> {
    Schema::new(
        columns
            .iter()
            .map(|(n, t)| Field::new(n.clone(), *t))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("ts", ColumnType::TimestampNs),
            Field::new("sensor", ColumnType::DictU32),
            Field::new("temp", ColumnType::FixedPoint { scale: 8 }),
            Field::new("count", ColumnType::Int64),
            Field::new("ok", ColumnType::U8),
            Field::new("load", ColumnType::Float64),
        ])
        .unwrap()
    }

    #[test]
    fn json_mapping_with_paths_and_units() {
        let map: HashMap<String, String> = [
            ("ts", "meta.time"),
            ("sensor", "meta.id"),
            ("temp", "values.temp"),
            ("count", "values.n"),
            ("ok", "values.ok"),
            ("load", "values.load"),
        ]
        .into_iter()
        .map(|(a, b)| (a.to_owned(), b.to_owned()))
        .collect();
        let mut m =
            RowMapper::new(schema(), &map, false, TsUnit::Millis, false, Some("sensor")).unwrap();
        let mut dicts = RowMapper::dictionaries_for(&schema());
        let msg = br#"{"meta":{"time":1762700000123,"id":"rack-7"},"values":{"temp":"21.5","n":42,"ok":true,"load":0.25}}"#;
        let (key, row) = m.map(msg, 0, false, &mut dicts).unwrap();
        let row = row.to_vec();
        assert_eq!(row[0], Value::I64(1_762_700_000_123_000_000));
        assert_eq!(row[1], Value::U32(0));
        assert_eq!(row[2], Value::I64(21_50000000));
        assert_eq!(row[3], Value::I64(42));
        assert_eq!(row[4], Value::U8(1));
        assert_eq!(row[5], Value::F64(0.25));
        assert_eq!(key, 0);
        let msg2 = br#"{"meta":{"time":1762700000124,"id":"rack-9"},"values":{"temp":22,"n":1,"ok":false,"load":1}}"#;
        let (key, row) = m.map(msg2, 0, false, &mut dicts).unwrap();
        let row = row.to_vec();
        assert_eq!(row[1], Value::U32(1));
        assert_eq!(row[2], Value::I64(22_00000000));
        assert_eq!(key, 1);
        assert_eq!(dicts[1].as_ref().unwrap().name(SymbolId(1)), Some("rack-9"));
        // missing field, bad json
        assert!(matches!(
            m.map(br#"{"meta":{"time":1}}"#, 0, false, &mut dicts),
            Err(MapError::Missing(_))
        ));
        assert!(matches!(
            m.map(b"not json", 0, false, &mut dicts),
            Err(MapError::Malformed(_))
        ));
    }

    #[test]
    fn nullable_columns_take_missing_values() {
        let schema = Schema::new(vec![
            Field::new("ts", ColumnType::TimestampNs),
            Field::new("meter", ColumnType::DictU32),
            Field::nullable("kw", ColumnType::Float64),
            Field::nullable("pulses", ColumnType::Int64),
            Field::new("phase", ColumnType::U8),
        ])
        .unwrap();
        let map: HashMap<String, String> = [
            ("ts", "t"),
            ("meter", "m"),
            ("kw", "kw"),
            ("pulses", "p"),
            ("phase", "ph"),
        ]
        .into_iter()
        .map(|(a, b)| (a.to_owned(), b.to_owned()))
        .collect();
        let mut m =
            RowMapper::new(schema.clone(), &map, false, TsUnit::Nanos, false, None).unwrap();
        let mut dicts = RowMapper::dictionaries_for(&schema);
        let (_, row) = m
            .map(br#"{"t":5,"m":"a","kw":null,"ph":1}"#, 0, false, &mut dicts)
            .unwrap();
        assert_eq!(row[2], Value::Null);
        assert_eq!(row[3], Value::Null);
        assert_eq!(row[4], Value::U8(1));
        // a value that is present but wrong is still an error, and a non-null column still
        // needs its value
        assert!(
            m.map(br#"{"t":6,"m":"a","kw":"x","ph":1}"#, 0, false, &mut dicts)
                .is_err()
        );
        assert!(matches!(
            m.map(br#"{"t":6,"m":"a","kw":1.5}"#, 0, false, &mut dicts),
            Err(MapError::Missing(_))
        ));
        // CSV: an empty field
        let csv_map: HashMap<String, String> = [
            ("ts", "0"),
            ("meter", "1"),
            ("kw", "2"),
            ("pulses", "3"),
            ("phase", "4"),
        ]
        .into_iter()
        .map(|(a, b)| (a.to_owned(), b.to_owned()))
        .collect();
        let mut c = RowMapper::new(schema, &csv_map, true, TsUnit::Nanos, false, None).unwrap();
        let (_, row) = c.map(b"7,a,,12,0", 0, true, &mut dicts).unwrap();
        assert_eq!(row[2], Value::Null);
        assert_eq!(row[3], Value::I64(12));
    }

    #[test]
    fn topic_segments_map_to_columns() {
        let schema = Schema::new(vec![
            Field::new("ts", ColumnType::TimestampNs),
            Field::new("line", ColumnType::DictU32),
            Field::new("machine", ColumnType::DictU32),
            Field::new("topic", ColumnType::DictU32),
            Field::new("kw", ColumnType::Float64),
        ])
        .unwrap();
        let map: HashMap<String, String> = [
            ("ts", "t"),
            ("line", "$topic.1"),
            ("machine", "$topic.2"),
            ("topic", "$topic"),
            ("kw", "kw"),
        ]
        .into_iter()
        .map(|(a, b)| (a.to_owned(), b.to_owned()))
        .collect();
        let mut m = RowMapper::new(
            schema.clone(),
            &map,
            false,
            TsUnit::Nanos,
            false,
            Some("machine"),
        )
        .unwrap()
        .with_topic_prefix(true);
        let mut dicts = RowMapper::dictionaries_for(&schema);
        let (key, row) = m
            .map(
                b"plant1/line3/press7/power\0{\"t\":9,\"kw\":12.5}",
                0,
                false,
                &mut dicts,
            )
            .unwrap();
        let row = row.to_vec();
        assert_eq!(key, 0);
        assert_eq!(row[4], Value::F64(12.5));
        assert_eq!(dicts[1].as_ref().unwrap().name(SymbolId(0)), Some("line3"));
        assert_eq!(dicts[2].as_ref().unwrap().name(SymbolId(0)), Some("press7"));
        assert_eq!(
            dicts[3].as_ref().unwrap().name(SymbolId(0)),
            Some("plant1/line3/press7/power")
        );
        // a topic with too few segments leaves the column without a value
        assert!(matches!(
            m.map(b"plant1\0{\"t\":10,\"kw\":1}", 0, false, &mut dicts),
            Err(MapError::Missing(_))
        ));
        assert!(m.map(b"no separator", 0, false, &mut dicts).is_err());
        assert!(
            RowMapper::new(
                schema,
                &[("line".to_owned(), "$topic.x".to_owned())].into(),
                false,
                TsUnit::Nanos,
                true,
                None
            )
            .is_err()
        );
    }

    #[test]
    fn csv_mapping_and_receive_time() {
        let map: HashMap<String, String> = [
            ("sensor", "0"),
            ("temp", "1"),
            ("count", "2"),
            ("ok", "3"),
            ("load", "4"),
        ]
        .into_iter()
        .map(|(a, b)| (a.to_owned(), b.to_owned()))
        .collect();
        let mut m = RowMapper::new(schema(), &map, true, TsUnit::Nanos, true, None).unwrap();
        let mut dicts = RowMapper::dictionaries_for(&schema());
        let (_, row) = m
            .map(b"a1, 19.25, 7, 0, 0.5\n", 999, true, &mut dicts)
            .unwrap();
        let row = row.to_vec();
        assert_eq!(row[0], Value::I64(999));
        assert_eq!(row[2], Value::I64(19_25000000));
        assert_eq!(row[3], Value::I64(7));
        assert_eq!(row[5], Value::F64(0.5));
    }

    #[test]
    fn rfc3339() {
        assert_eq!(parse_rfc3339_ns("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_rfc3339_ns("1970-01-01T00:00:01.5Z"),
            Some(1_500_000_000)
        );
        assert_eq!(
            parse_rfc3339_ns("2026-11-09T18:19:22.908Z"),
            Some(1_794_248_362_908_000_000)
        );
        assert_eq!(
            parse_rfc3339_ns("2026-11-09T19:19:22+01:00"),
            Some(1_794_248_362_000_000_000)
        );
        assert_eq!(
            parse_rfc3339_ns("2026-11-09 18:19:22.000000001Z"),
            Some(1_794_248_362_000_000_001)
        );
        assert_eq!(parse_rfc3339_ns("2026-13-09T18:19:22Z"), None);
        assert_eq!(parse_rfc3339_ns("garbage"), None);
        // strict: no signs, no impossible dates, nothing after the offset
        assert_eq!(parse_rfc3339_ns("2026-02-30T00:00:00Z"), None);
        assert_eq!(parse_rfc3339_ns("2026-02-29T00:00:00Z"), None);
        assert!(parse_rfc3339_ns("2028-02-29T00:00:00Z").is_some());
        assert_eq!(parse_rfc3339_ns("+026-11-09T18:19:22Z"), None);
        assert_eq!(parse_rfc3339_ns("2026-11-09T18:19:22Zjunk"), None);
        assert_eq!(parse_rfc3339_ns("2026-11-09T18:19:22+01:00x"), None);
        assert_eq!(parse_rfc3339_ns("2026-11-09T18:19:22+0100"), None);
        assert_eq!(
            parse_rfc3339_ns("2026-11-09T18:19:22"),
            parse_rfc3339_ns("2026-11-09T18:19:22Z")
        );
    }

    fn mapper(csv: bool, cols: &[(&str, &str)], unit: TsUnit) -> RowMapper {
        let map: HashMap<String, String> = cols
            .iter()
            .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
            .collect();
        RowMapper::new(schema(), &map, csv, unit, true, Some("sensor")).unwrap()
    }

    const ALL: [(&str, &str); 5] = [
        ("sensor", "s"),
        ("temp", "t"),
        ("count", "n"),
        ("ok", "ok"),
        ("load", "l"),
    ];

    #[test]
    fn json_floats_round_instead_of_rejecting() {
        let mut m = mapper(false, &ALL, TsUnit::Nanos);
        let mut d = RowMapper::dictionaries_for(&schema());
        let (_, row) = m
            .map(
                br#"{"s":"a","t":21.700000000000003,"n":1,"ok":1,"l":1e-3}"#,
                5,
                false,
                &mut d,
            )
            .unwrap();
        assert_eq!(row[2], Value::I64(21_70000000));
        assert_eq!(row[5], Value::F64(0.001));
        let (_, row) = m
            .map(
                br#"{"s":"a","t":1.5e2,"n":1,"ok":1,"l":2}"#,
                6,
                false,
                &mut d,
            )
            .unwrap();
        assert_eq!(row[2], Value::I64(150_00000000));
        // out of range stays an error
        assert!(
            m.map(
                br#"{"s":"a","t":1e300,"n":1,"ok":1,"l":2}"#,
                7,
                false,
                &mut d
            )
            .is_err()
        );
        // decimal seconds keep nanosecond precision
        let mut m = mapper(
            false,
            &[
                ("ts", "ts"),
                ("sensor", "s"),
                ("temp", "t"),
                ("count", "n"),
                ("ok", "ok"),
                ("load", "l"),
            ],
            TsUnit::Seconds,
        );
        let (_, row) = m
            .map(
                br#"{"ts":1762700000.123,"s":"a","t":1,"n":1,"ok":1,"l":2}"#,
                0,
                false,
                &mut d,
            )
            .unwrap();
        assert_eq!(row[0], Value::I64(1_762_700_000_123_000_000));
    }

    #[test]
    fn dictionary_values_are_bounded() {
        let mut m = mapper(false, &ALL, TsUnit::Nanos).with_max_dict_entries(2);
        let mut d = RowMapper::dictionaries_for(&schema());
        let msg = |s: &str| format!(r#"{{"s":{s},"t":1,"n":1,"ok":1,"l":2}}"#);
        assert!(m.map(msg("\"a\"").as_bytes(), 1, false, &mut d).is_ok());
        assert!(m.map(msg("7").as_bytes(), 1, false, &mut d).is_ok());
        assert_eq!(d[1].as_ref().unwrap().name(SymbolId(1)), Some("7"));
        // full: known values still map, new ones are rejected
        assert!(m.map(msg("\"a\"").as_bytes(), 1, false, &mut d).is_ok());
        assert!(m.map(msg("\"c\"").as_bytes(), 1, false, &mut d).is_err());
        assert_eq!(d[1].as_ref().unwrap().len(), 2);
        // objects, arrays, null and oversized strings are not keys
        let mut m = mapper(false, &ALL, TsUnit::Nanos);
        for bad in ["{\"x\":1}", "[1,2]", "null"] {
            assert!(
                m.map(msg(bad).as_bytes(), 1, false, &mut d).is_err(),
                "{bad}"
            );
        }
        let long = format!("\"{}\"", "k".repeat(MAX_DICT_VALUE_BYTES + 1));
        assert!(m.map(msg(&long).as_bytes(), 1, false, &mut d).is_err());
        assert_eq!(d[1].as_ref().unwrap().len(), 2);
    }

    /// A message that is rejected adds nothing to a dictionary, whichever column was wrong and
    /// whoever rejected it.
    #[test]
    fn rejected_rows_leave_no_dictionary_entries() {
        let mut m = mapper(false, &ALL, TsUnit::Nanos);
        let mut d = RowMapper::dictionaries_for(&schema());
        // the key converts, a later column does not
        assert!(
            m.map(
                br#"{"s":"ghost-1","t":"oops","n":1,"ok":1,"l":2}"#,
                1,
                false,
                &mut d
            )
            .is_err()
        );
        assert!(
            m.map(
                br#"{"s":"ghost-2","t":1,"n":1,"ok":999,"l":2}"#,
                1,
                false,
                &mut d
            )
            .is_err()
        );
        assert_eq!(d[1].as_ref().unwrap().len(), 0);
        // the caller rejects after parsing (ordering, future timestamps): still nothing
        let p = m
            .parse(br#"{"s":"ghost-3","t":1,"n":1,"ok":1,"l":2}"#, 7, false, &d)
            .unwrap();
        assert_eq!(p, Parsed { ts: 7, key: None });
        assert_eq!(d[1].as_ref().unwrap().len(), 0);
        // accepted: interned by finish, and known on the next parse
        let (key, row) = m.finish(&mut d);
        assert_eq!((key, row[1]), (0, Value::U32(0)));
        let p = m
            .parse(br#"{"s":"ghost-3","t":1,"n":1,"ok":1,"l":2}"#, 8, false, &d)
            .unwrap();
        assert_eq!(
            p,
            Parsed {
                ts: 8,
                key: Some(0)
            }
        );
        // a pending value of an abandoned row does not leak into the next one
        m.parse(br#"{"s":"never","t":1,"n":1,"ok":1,"l":2}"#, 9, false, &d)
            .unwrap();
        let (key, _) = m
            .map(
                br#"{"s":"ghost-3","t":1,"n":1,"ok":1,"l":2}"#,
                9,
                false,
                &mut d,
            )
            .unwrap();
        assert_eq!(key, 0);
        assert_eq!(d[1].as_ref().unwrap().len(), 1);
    }

    #[test]
    fn csv_quotes() {
        let mut m = mapper(
            true,
            &[
                ("sensor", "0"),
                ("temp", "1"),
                ("count", "2"),
                ("ok", "3"),
                ("load", "4"),
            ],
            TsUnit::Nanos,
        );
        let mut d = RowMapper::dictionaries_for(&schema());
        let (_, row) = m
            .map(b"\"rack 7, row B\", \"1.5\",3,1,0.5", 1, true, &mut d)
            .unwrap();
        assert_eq!(row[2], Value::I64(1_50000000));
        assert_eq!(
            d[1].as_ref().unwrap().name(SymbolId(0)),
            Some("rack 7, row B")
        );
        assert!(m.map(b"\"open,1,2,3,4", 1, true, &mut d).is_err());
        assert!(m.map(b"\"a\"x,1,2,3,4", 1, true, &mut d).is_err());
        assert!(m.map(b"a,1,2", 1, true, &mut d).is_err());
    }
}
