//! Generic ingest ("bring your own feed"): map JSON or CSV messages onto a user-defined
//! schema (`[[tables]]` + `[[sources]]` in the server config) and produce store rows.
//!
//! The mapper allocates nothing per message after warm-up: JSON goes through a path-directed
//! single-pass scanner ([`crate::json_scan`]: no document tree, unmapped fields skipped at memory
//! speed), values are converted from byte spans of the message, decimals are parsed with
//! [`parse_fixed`] (no floats for `fixed8`), strings go through a per-column [`SymbolDict`], and the
//! output rows live in a buffer that is reused. With `rows`, one message yields one row per
//! element of an array. Sparkplug B payloads ([`RowMapper::with_sparkplug`]) yield one row per
//! metric, decoded in place by [`crate::sparkplug`].
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

use crate::json_scan;
use crate::sparkplug::{self, Definitions};
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
    /// A field of a Sparkplug B metric or of its message (`$metric`, `$value`, …).
    Sparkplug(SpField),
    /// Not mapped: timestamp from receive time, or a default/null.
    Absent,
}

/// What a Sparkplug selector reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpField {
    /// `$metric`: the metric name (from the message, or its alias resolved through the BIRTH).
    Metric,
    /// `$value`: the value. Numbers and booleans go to numeric columns (a string value is `NULL`
    /// there); a `dict` column takes any value as text.
    Value,
    /// `$string`: the value when it is a string, `NULL` otherwise.
    String,
    /// `$timestamp`: the metric's timestamp, else the payload's (ms since the epoch).
    Timestamp,
    /// `$datatype`: the Sparkplug datatype code.
    Datatype,
    /// `$historical`: 1 for a historical (late, buffered) value.
    Historical,
    /// `$seq`: the payload sequence number.
    Seq,
    /// `$group`: the group id of the topic.
    Group,
    /// `$node`: the edge node id.
    Node,
    /// `$device`: the device id (`NULL` for node-level metrics).
    Device,
    /// `$type`: the message type (`NBIRTH`, `NDATA`, `DBIRTH`, `DDATA`).
    Type,
}

impl SpField {
    /// The field a `map` value names, if it is a Sparkplug selector.
    #[must_use]
    pub fn parse(spec: &str) -> Option<Self> {
        Some(match spec {
            "$metric" => Self::Metric,
            "$value" => Self::Value,
            "$string" => Self::String,
            "$timestamp" => Self::Timestamp,
            "$datatype" => Self::Datatype,
            "$historical" => Self::Historical,
            "$seq" => Self::Seq,
            "$group" => Self::Group,
            "$node" => Self::Node,
            "$device" => Self::Device,
            "$type" => Self::Type,
            _ => return None,
        })
    }
}

/// Sparkplug state of a mapper: what the BIRTH messages of its edge nodes defined.
#[derive(Debug, Default, Clone)]
struct SpState {
    defs: Definitions,
    /// Per column: the selector has one value per message (`$group`, `$node`, `$device`,
    /// `$type`, `$seq`, `$topic`), so it is converted once and copied into the later rows.
    per_message: Vec<bool>,
    /// Per column, in the current message: the row that holds its converted value.
    converted_in: Vec<Option<usize>>,
}

/// A Sparkplug value before conversion to a column type.
#[derive(Debug, Clone, Copy)]
enum SpRaw<'a> {
    Text(&'a str),
    Int(i64),
    UInt(u64),
    Float(f64),
    Bool(bool),
    /// Milliseconds since the epoch (timestamps, `DateTime` values).
    Ms(u64),
}

/// Most rows one message may carry with `rows`: bounds the work and memory one message can
/// cause.
pub const MAX_ROWS_PER_MESSAGE: usize = 65_536;

/// Where a column's value comes from once the JSON paths are compiled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColSrc {
    /// A message-level path (slot of the scan).
    Msg(usize),
    /// A path inside each element of the `rows` array (slot of the scan).
    Elem(usize),
    /// Not a JSON path (topic, CSV, absent).
    Other,
}

/// Compiled mapping for one table.
#[derive(Debug, Clone)]
pub struct RowMapper {
    schema: Schema,
    selectors: Vec<Selector>,
    ts_unit: TsUnit,
    ts_from_receive: bool,
    /// Cap on distinct values per dictionary column.
    max_dict_entries: usize,
    /// Messages arrive as `topic \0 payload` (MQTT sources).
    topic_prefixed: bool,
    /// JSON: the mapped paths compiled into one scan, and per column which slot feeds it.
    plan: json_scan::Plan,
    col_src: Vec<ColSrc>,
    /// `rows = "<path>"`: one row per element of that array.
    rows_path: Option<Vec<String>>,
    /// Scan result of the last message (spans into it); reused.
    found: json_scan::Found,
    /// Scratch CSV field ranges.
    csv_fields: Vec<(usize, usize)>,
    /// What the last message mapped to (see [`Mapped`]); its buffers are reused.
    out: Mapped,
    /// Per column: the distinct new dictionary values of the last message, counted against
    /// `max_dict_entries` so that one message of many rows cannot overshoot the cap.
    new_names: Vec<std::collections::HashSet<Box<str>>>,
    /// Mapping without the dictionaries (a parallel worker): per column, the ids this mapper has
    /// learned from results the dictionaries' owner resolved ([`Self::learn`]). Ids never change
    /// once given, so an entry is never stale; it saves the owner one hash per value.
    learned: Vec<HashMap<Box<str>, u32, foldhash::fast::RandomState>>,
    /// `format = "sparkplug"`: payloads are Sparkplug B, one row per metric.
    sparkplug: Option<Box<SpState>>,
}

/// What [`RowMapper::parsed`] found for one row, before anything was added to a dictionary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parsed {
    /// Timestamp of the row, ns.
    pub ts: i64,
    /// Partition key when its value is already in the dictionary (0 for a table without a
    /// partition column); `None` when this row introduces a new key.
    pub key: Option<u32>,
}

/// Mapping error (message or row rejected, counted by the caller).
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
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

impl From<json_scan::ScanError> for MapError {
    fn from(e: json_scan::ScanError) -> Self {
        match e {
            json_scan::ScanError::Invalid => Self::Malformed("invalid json"),
            json_scan::ScanError::TooDeep => Self::Malformed("json nested too deeply"),
            json_scan::ScanError::TooManyRows => Self::Malformed("too many rows in one message"),
        }
    }
}

/// A raw value before conversion: text (CSV field, topic segment) or a JSON span.
#[derive(Debug, Clone, Copy)]
enum Scalar<'a> {
    Text(&'a str),
    Json(json_scan::Span),
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
            if let Some(sp) = map.get(&f.name).and_then(|s| SpField::parse(s.trim())) {
                selectors.push(Selector::Sparkplug(sp));
                continue;
            }
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
                Some(spec) => {
                    let path: Vec<String> = spec.trim().split('.').map(str::to_owned).collect();
                    if path.iter().any(String::is_empty) || path == ["$msg"] {
                        return Err(MapError::Convert {
                            column: f.name.clone(),
                            reason: format!("{spec:?} is not a JSON path"),
                        });
                    }
                    Selector::Path(path)
                }
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
        let (plan, col_src) = Self::compile(&selectors, None);
        let out = Mapped::new(schema.clone(), partition_col);
        Ok(Self {
            schema,
            selectors,
            ts_unit,
            ts_from_receive,
            max_dict_entries: DEFAULT_MAX_DICT_ENTRIES,
            topic_prefixed: false,
            plan,
            col_src,
            rows_path: None,
            found: json_scan::Found::default(),
            csv_fields: Vec::new(),
            out,
            new_names: Vec::new(),
            learned: Vec::new(),
            sparkplug: None,
        })
    }

    /// Payloads are Sparkplug B (`format = "sparkplug"`, MQTT sources): one row per metric of
    /// every BIRTH and DATA message; columns are mapped with `$metric`, `$value`, `$string`,
    /// `$timestamp`, `$datatype`, `$historical`, `$seq`, `$group`, `$node`, `$device`, `$type`
    /// and `$topic`. With `on == false` those selectors are refused.
    pub fn with_sparkplug(mut self, on: bool) -> Result<Self, MapError> {
        let names = self.schema.fields().iter().map(|f| f.name.as_str());
        for (sel, name) in self.selectors.iter().zip(names) {
            let bad = match sel {
                Selector::Sparkplug(_) if !on => "Sparkplug selectors need format = \"sparkplug\"",
                Selector::Path(_) | Selector::Index(_) if on => {
                    "format = \"sparkplug\" maps columns with $metric, $value, $timestamp, … (not JSON paths)"
                }
                _ => continue,
            };
            return Err(MapError::Convert {
                column: name.to_owned(),
                reason: bad.to_owned(),
            });
        }
        if on {
            if self.rows_path.is_some() {
                return Err(MapError::Convert {
                    column: "rows".into(),
                    reason: "rows needs format = \"json\"".into(),
                });
            }
            let per_message = self
                .selectors
                .iter()
                .map(|s| {
                    matches!(
                        s,
                        Selector::Topic(_)
                            | Selector::Sparkplug(
                                SpField::Group
                                    | SpField::Node
                                    | SpField::Device
                                    | SpField::Type
                                    | SpField::Seq
                            )
                    )
                })
                .collect();
            self.sparkplug = Some(Box::new(SpState {
                per_message,
                ..SpState::default()
            }));
        }
        Ok(self)
    }

    /// The Sparkplug definitions known so far (`None` unless [`Self::with_sparkplug`]).
    #[must_use]
    pub fn sparkplug_definitions(&self) -> Option<&Definitions> {
        self.sparkplug.as_ref().map(|s| &s.defs)
    }

    /// Replace the Sparkplug definitions (a snapshot read back from the log).
    pub fn set_sparkplug_definitions(&mut self, defs: Definitions) {
        if let Some(s) = &mut self.sparkplug {
            s.defs = defs;
        }
    }

    /// Compile the JSON selectors: message paths, and with `rows` the element paths (a path
    /// starting with `$msg.` stays at message level).
    fn compile(selectors: &[Selector], rows: Option<&[String]>) -> (json_scan::Plan, Vec<ColSrc>) {
        let mut msg_paths: Vec<Vec<String>> = Vec::new();
        let mut elem_paths: Vec<Vec<String>> = Vec::new();
        let mut col_src = Vec::with_capacity(selectors.len());
        for sel in selectors {
            col_src.push(match sel {
                Selector::Path(p) if p.first().is_some_and(|s| s == "$msg") => {
                    msg_paths.push(p[1..].to_vec());
                    ColSrc::Msg(msg_paths.len() - 1)
                }
                Selector::Path(p) if rows.is_some() => {
                    elem_paths.push(p.clone());
                    ColSrc::Elem(elem_paths.len() - 1)
                }
                Selector::Path(p) => {
                    msg_paths.push(p.clone());
                    ColSrc::Msg(msg_paths.len() - 1)
                }
                _ => ColSrc::Other,
            });
        }
        (
            json_scan::Plan::new(&msg_paths, rows, &elem_paths, MAX_ROWS_PER_MESSAGE),
            col_src,
        )
    }

    /// One row per element of the JSON array at `rows` (a dotted path): column paths are read
    /// inside each element, paths starting with `$msg.` from the message around it.
    pub fn with_rows(mut self, rows: Option<&str>) -> Result<Self, MapError> {
        let Some(rows) = rows else {
            return Ok(self);
        };
        let path: Vec<String> = rows.trim().split('.').map(str::to_owned).collect();
        if path.iter().any(String::is_empty) || path.first().is_some_and(|s| s.starts_with('$')) {
            return Err(MapError::Convert {
                column: "rows".into(),
                reason: format!("{rows:?} is not a JSON path"),
            });
        }
        if self
            .selectors
            .iter()
            .any(|s| matches!(s, Selector::Index(_)))
        {
            return Err(MapError::Convert {
                column: "rows".into(),
                reason: "rows needs format = \"json\"".into(),
            });
        }
        let (plan, col_src) = Self::compile(&self.selectors, Some(&path));
        self.plan = plan;
        self.col_src = col_src;
        self.rows_path = Some(path);
        Ok(self)
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

    /// Cap the number of distinct values per dictionary column (default
    /// [`DEFAULT_MAX_DICT_ENTRIES`]); rows that would add a value beyond it are rejected.
    #[must_use]
    pub fn with_max_dict_entries(mut self, n: usize) -> Self {
        self.max_dict_entries = n.max(1);
        self
    }

    /// Map a message that carries one row using the table's `dicts` (from
    /// [`Self::dictionaries_for`]). Returns the partition key and the row (valid until the next
    /// call). Nothing is added to a dictionary unless the row is accepted.
    pub fn map(
        &mut self,
        msg: &[u8],
        recv_ns: i64,
        csv: bool,
        dicts: &mut [Option<SymbolDict>],
    ) -> Result<(u32, &[Value]), MapError> {
        let n = self.parse(msg, recv_ns, csv, dicts)?;
        if n == 0 {
            return Err(MapError::Missing("rows".into()));
        }
        self.parsed(0)?;
        Ok(self.finish_row(0, dicts))
    }

    /// Convert one message into rows without touching the dictionaries: a row that is refused
    /// (here, or by the caller's own checks on [`Self::parsed`]) leaves no trace. Returns the
    /// number of rows; an error here refuses the whole message (malformed, or the `rows` array
    /// missing). Rows are then read with [`Self::parsed`] and taken with [`Self::finish_row`].
    pub fn parse(
        &mut self,
        msg: &[u8],
        recv_ns: i64,
        csv: bool,
        dicts: &[Option<SymbolDict>],
    ) -> Result<usize, MapError> {
        // the result describes this message whatever path returns: a message that maps to no
        // row (or is refused) must not leave the previous message's rows behind
        self.out.n_rows = 0;
        self.out.row_err.clear();
        let (topic, payload) = if self.topic_prefixed {
            let cut =
                memchr::memchr(0, msg).ok_or(MapError::Malformed("message without its topic"))?;
            let topic = std::str::from_utf8(&msg[..cut])
                .map_err(|_| MapError::Malformed("topic is not utf-8"))?;
            (topic, &msg[cut + 1..])
        } else {
            ("", msg)
        };
        if let Some(mut sp) = self.sparkplug.take() {
            let r = self.parse_sparkplug(topic, payload, recv_ns, dicts, &mut sp);
            self.sparkplug = Some(sp);
            return r;
        }
        let ncols = self.schema.fields().len();
        if csv {
            let text = std::str::from_utf8(payload)
                .map_err(|_| MapError::Malformed("csv is not utf-8"))?;
            let text = text.trim_end_matches(['\r', '\n']);
            split_csv(text, &mut self.csv_fields)?;
            self.begin(1, ncols);
            for c in 0..ncols {
                let raw = match &self.selectors[c] {
                    Selector::Index(k) => self.csv_fields.get(*k).map(|&(a, b)| text[a..b].trim()),
                    Selector::Topic(n) => topic_segment(topic, *n),
                    Selector::Path(_) | Selector::Sparkplug(_) | Selector::Absent => None,
                };
                if let Err(e) = self.convert(0, c, raw.map(Scalar::Text), payload, recv_ns, dicts) {
                    self.out.row_err[0] = Some(e);
                    break;
                }
            }
            return Ok(1);
        }
        self.plan.scan(payload, &mut self.found)?;
        let n_rows = if let Some(path) = &self.rows_path {
            if !self.found.rows_seen {
                return Err(MapError::Missing(path.join(".")));
            }
            self.found.rows
        } else {
            1
        };
        self.begin(n_rows, ncols);
        let elem_slots = self.plan.elem_slots();
        for r in 0..n_rows {
            for c in 0..ncols {
                let raw = match self.col_src[c] {
                    ColSrc::Msg(i) => self.found.msg[i].map(Scalar::Json),
                    ColSrc::Elem(j) => self.found.elems[r * elem_slots + j].map(Scalar::Json),
                    ColSrc::Other => match &self.selectors[c] {
                        Selector::Topic(n) => topic_segment(topic, *n).map(Scalar::Text),
                        _ => None,
                    },
                };
                if let Err(e) = self.convert(r, c, raw, payload, recv_ns, dicts) {
                    self.out.row_err[r] = Some(e);
                    break;
                }
            }
        }
        Ok(n_rows)
    }

    /// One row per metric of a Sparkplug BIRTH or DATA message; other messages (DEATH,
    /// commands, `STATE`, topics outside `spBv1.0`) give none. A BIRTH records each metric's
    /// datatype, transient flag and alias ([`Definitions`]; an NBIRTH first forgets what the node
    /// defined before, a DBIRTH what the device did); DATA metrics are read with them. Transient
    /// metrics are not stored.
    fn parse_sparkplug(
        &mut self,
        topic: &str,
        payload: &[u8],
        recv_ns: i64,
        dicts: &[Option<SymbolDict>],
        sp: &mut SpState,
    ) -> Result<usize, MapError> {
        let Some(t) = sparkplug::Topic::parse(topic) else {
            return Ok(0);
        };
        match t.kind {
            "NBIRTH" => sp.defs.reset_node(t.group, t.node),
            "DBIRTH" => sp.defs.reset_device(t.group, t.node, t.device),
            _ => {}
        }
        if !t.has_readings() {
            return Ok(0);
        }
        let h = sparkplug::header(payload).map_err(|e| MapError::Malformed(e.0))?;
        let ncols = self.schema.fields().len();
        self.begin(h.metrics, ncols);
        let mut defs = if t.is_birth() {
            Defs::Birth(&mut sp.defs)
        } else {
            Defs::Data(sp.defs.node(t.group, t.node))
        };
        let per_message = &sp.per_message;
        let converted_in = &mut sp.converted_in;
        converted_in.clear();
        converted_in.resize(ncols, None);
        let mut r = 0usize;
        let walked = sparkplug::for_each_metric(payload, |m| {
            let found = sp_definition(&mut defs, &t, m);
            let row = match found {
                Ok(None) => return, // transient
                Ok(Some((name, m))) => (|| {
                    for c in 0..ncols {
                        // the same for every row of the message: copy the converted cell
                        if per_message[c]
                            && let Some(from) = converted_in[c]
                        {
                            self.copy_cell(from, r, c);
                            continue;
                        }
                        let raw = match self.selectors[c] {
                            Selector::Sparkplug(f) => sp_raw(f, &m, name, &h, &t)?,
                            Selector::Topic(n) => topic_segment(topic, n).map(SpRaw::Text),
                            _ => None,
                        };
                        let from_value = self.selectors[c] == Selector::Sparkplug(SpField::Value);
                        self.convert_sp(r, c, raw, from_value, recv_ns, dicts)?;
                        if per_message[c] {
                            converted_in[c] = Some(r);
                        }
                    }
                    Ok(())
                })(),
                Err(e) => Err(e),
            };
            if let Err(e) = row {
                self.out.row_err[r] = Some(e);
            }
            r += 1;
        });
        walked.map_err(|e| MapError::Malformed(e.0))?;
        self.out.n_rows = r;
        self.out.row_err.truncate(r);
        Ok(r)
    }

    /// Copy cell (`from`, `c`) into (`to`, `c`), with its pending dictionary value.
    fn copy_cell(&mut self, from: usize, to: usize, c: usize) {
        let ncols = self.schema.fields().len();
        let (a, b) = (from * ncols + c, to * ncols + c);
        self.out.rows[b] = self.out.rows[a];
        self.out.pending_set[b] = self.out.pending_set[a];
        if self.out.pending_set[a] {
            let (lo, hi) = self.out.pending.split_at_mut(b);
            hi[0].clone_from(&lo[a]);
        }
    }

    /// Convert a Sparkplug value into cell (`r`, `c`). `from_value`: the column maps `$value`,
    /// where a string metric counts as no value for a numeric column.
    fn convert_sp(
        &mut self,
        r: usize,
        c: usize,
        raw: Option<SpRaw<'_>>,
        from_value: bool,
        recv_ns: i64,
        dicts: &[Option<SymbolDict>],
    ) -> Result<(), MapError> {
        use std::fmt::Write as _;
        let ncols = self.schema.fields().len();
        let idx = r * ncols + c;
        let f: &Field = &self.schema.fields()[c];
        let raw = match raw {
            Some(SpRaw::Text(_)) if from_value && f.ty != ColumnType::DictU32 => None,
            x => x,
        };
        let Some(raw) = raw else {
            if f.nullable && c > 0 {
                self.out.rows[idx] = Value::Null;
                return Ok(());
            }
            if c == 0 && self.ts_from_receive {
                self.out.rows[idx] = Value::I64(recv_ns);
                return Ok(());
            }
            // a key column always holds a value: "" where the selector does not apply
            // (`$device` of a node metric, `$string` of a number)
            if f.ty == ColumnType::DictU32 {
                return self.dict_value(idx, c, "", dicts);
            }
            return Err(MapError::Missing(f.name.clone()));
        };
        if f.ty == ColumnType::DictU32 {
            let mut buf = StackStr::default();
            let text = match raw {
                SpRaw::Text(s) => Ok(s),
                SpRaw::Bool(b) => Ok(if b { "true" } else { "false" }),
                SpRaw::Int(v) => write!(buf, "{v}").map(|()| buf.as_str()),
                SpRaw::UInt(v) | SpRaw::Ms(v) => write!(buf, "{v}").map(|()| buf.as_str()),
                SpRaw::Float(x) if x.is_finite() => write!(buf, "{x}").map(|()| buf.as_str()),
                SpRaw::Float(_) => Err(std::fmt::Error),
            };
            let Ok(text) = text else {
                return Err(MapError::Convert {
                    column: f.name.clone(),
                    reason: "value has no text form".into(),
                });
            };
            return self.dict_value(idx, c, text, dicts);
        }
        let value = sp_typed(f.ty, raw).map_err(|reason| MapError::Convert {
            column: f.name.clone(),
            reason: reason.to_owned(),
        })?;
        self.out.rows[idx] = value;
        Ok(())
    }

    /// Size the reused output buffers for `n_rows` rows.
    fn begin(&mut self, n_rows: usize, ncols: usize) {
        let cells = n_rows * ncols;
        self.out.n_rows = n_rows;
        self.out.asked.clear();
        if self.out.rows.len() < cells {
            self.out.rows.resize(cells, Value::Null);
        }
        if self.out.pending.len() < cells {
            self.out.pending.resize_with(cells, String::new);
        }
        self.out.pending_set.clear();
        self.out.pending_set.resize(cells, false);
        self.out.row_err.clear();
        self.out.row_err.resize(n_rows, None);
        if self.new_names.len() < ncols {
            self.new_names.resize_with(ncols, Default::default);
        }
        for set in &mut self.new_names {
            // `clear` rewrites the control bytes of every bucket even when the set is empty:
            // a memset per dictionary column per message, almost always for nothing
            if !set.is_empty() {
                set.clear();
            }
        }
    }

    /// Row `r` of the last message: its timestamp and partition key, or why it was refused.
    pub fn parsed(&self, r: usize) -> Result<Parsed, MapError> {
        self.out.parsed(r)
    }

    /// Take row `r` of the last message: add its new dictionary values and return its
    /// partition key and values. Call it only once the row is going to be stored.
    pub fn finish_row(&mut self, r: usize, dicts: &mut [Option<SymbolDict>]) -> (u32, &[Value]) {
        self.out.finish_row(r, dicts)
    }

    /// What the last message mapped to.
    #[must_use]
    pub fn mapped(&self) -> &Mapped {
        &self.out
    }

    /// The last message's result, mutably (to resolve and take its rows, see [`Mapped`]).
    pub fn mapped_mut(&mut self) -> &mut Mapped {
        &mut self.out
    }

    /// Exchange the last message's result with `other` (a used one, of any source, whose buffers
    /// this mapper then reuses): how a mapping worker hands a result over without copying or
    /// allocating. `other` takes this mapper's schema.
    pub fn swap_mapped(&mut self, other: &mut Mapped) {
        let o = &mut self.out;
        std::mem::swap(&mut o.rows, &mut other.rows);
        std::mem::swap(&mut o.n_rows, &mut other.n_rows);
        std::mem::swap(&mut o.row_err, &mut other.row_err);
        std::mem::swap(&mut o.pending, &mut other.pending);
        std::mem::swap(&mut o.pending_set, &mut other.pending_set);
        std::mem::swap(&mut o.asked, &mut other.asked);
        other.schema = o.schema.clone();
        other.partition_col = o.partition_col;
    }

    /// Learn the dictionary ids the owner gave to the values this mapper left pending in `m`
    /// (one of its own results, after it was applied): the next messages carry them already.
    /// Values of refused rows that were not interned are skipped. At most
    /// [`MAX_LEARNED_PER_COLUMN`] values per column are kept.
    pub fn learn(&mut self, m: &Mapped) {
        let ncols = self.schema.fields().len();
        if ncols == 0 {
            return;
        }
        if self.learned.len() < ncols {
            self.learned.resize_with(ncols, HashMap::default);
        }
        for &idx in &m.asked {
            let idx = idx as usize;
            let (Some(Value::U32(id)), Some(name)) = (m.rows.get(idx), m.pending.get(idx)) else {
                continue;
            };
            let map = &mut self.learned[idx % ncols];
            if *id != u32::MAX
                && map.len() < MAX_LEARNED_PER_COLUMN
                && !map.contains_key(name.as_str())
            {
                map.insert(name.as_str().into(), *id);
            }
        }
    }

    /// An empty result shaped for this mapper (a first buffer for [`Self::swap_mapped`]).
    #[must_use]
    pub fn empty_mapped(&self) -> Mapped {
        Mapped::new(self.out.schema.clone(), self.out.partition_col)
    }

    /// Cap on distinct values per dictionary column.
    #[must_use]
    pub fn max_dict_entries(&self) -> usize {
        self.max_dict_entries
    }

    /// Convert one raw value into cell (`r`, `c`).
    fn convert(
        &mut self,
        r: usize,
        c: usize,
        raw: Option<Scalar<'_>>,
        msg: &[u8],
        recv_ns: i64,
        dicts: &[Option<SymbolDict>],
    ) -> Result<(), MapError> {
        use json_scan::Kind as K;
        let ncols = self.schema.fields().len();
        let idx = r * ncols + c;
        let f: &Field = &self.schema.fields()[c];
        // the column name is only cloned on the error path
        let conv = |reason: &str| MapError::Convert {
            column: f.name.clone(),
            reason: reason.to_owned(),
        };
        // a nullable column without a value (field absent, JSON null, empty CSV field)
        let absent = match raw {
            None => true,
            Some(Scalar::Json(s)) => s.kind == K::Null,
            Some(Scalar::Text(s)) => s.is_empty(),
        };
        if absent && f.nullable && c > 0 {
            self.out.rows[idx] = Value::Null;
            return Ok(());
        }
        let Some(raw) = raw else {
            if c == 0 && self.ts_from_receive {
                self.out.rows[idx] = Value::I64(recv_ns);
                return Ok(());
            }
            return Err(MapError::Missing(f.name.clone()));
        };
        // text of a string (decoded when escaped: the only allocating case)
        let mut owned = String::new();
        let text: Option<&str> = match raw {
            Scalar::Text(s) => Some(s),
            Scalar::Json(s) if s.kind == K::Str => Some(
                std::str::from_utf8(s.bytes(msg))
                    .map_err(|_| MapError::Malformed("invalid json"))?,
            ),
            Scalar::Json(s) if s.kind == K::StrEscaped => {
                json_scan::unescape(s.bytes(msg), &mut owned)?;
                Some(owned.as_str())
            }
            Scalar::Json(_) => None,
        };
        let kind = match raw {
            Scalar::Json(s) => Some(s.kind),
            Scalar::Text(_) => None,
        };
        let num = match raw {
            Scalar::Json(s) if s.kind == K::Num => Some(s.bytes(msg)),
            _ => None,
        };
        if f.ty == ColumnType::DictU32 {
            let mut buf = StackStr::default();
            let name: &str = match (text, num, kind) {
                (Some(s), _, _) => s,
                (None, Some(n), _) => {
                    canonical_number(n, &mut buf).ok_or_else(|| conv("number too long"))?
                }
                (_, _, Some(K::True)) => "true",
                (_, _, Some(K::False)) => "false",
                // objects, arrays and null are not keys: interning their text would let one
                // producer grow the dictionary without bound
                _ => return Err(conv("expected a string or a number")),
            };
            return self.dict_value(idx, c, name, dicts);
        }
        let value = typed_value(f.ty, raw, text, num, kind, self.ts_unit).map_err(|r| conv(&r))?;
        self.out.rows[idx] = value;
        Ok(())
    }

    /// The id of a `dict` value: known ids at once, a new value kept aside for `finish_row`.
    fn dict_value(
        &mut self,
        idx: usize,
        c: usize,
        name: &str,
        dicts: &[Option<SymbolDict>],
    ) -> Result<(), MapError> {
        let f: &Field = &self.schema.fields()[c];
        let conv = |reason: &str| MapError::Convert {
            column: f.name.clone(),
            reason: reason.to_owned(),
        };
        if name.len() > MAX_DICT_VALUE_BYTES {
            return Err(conv("dictionary value longer than 256 bytes"));
        }
        // Mapped without the dictionaries (a parallel mapping worker): the name stays pending,
        // and the cap is checked when the result is resolved against them (`Mapped::resolve`)
        let Some(dict) = dicts.get(c).and_then(Option::as_ref) else {
            if let Some(&id) = self.learned.get(c).and_then(|m| m.get(name)) {
                self.out.rows[idx] = Value::U32(id);
                return Ok(());
            }
            self.out.pending[idx].clear();
            self.out.pending[idx].push_str(name);
            self.out.pending_set[idx] = true;
            self.out.rows[idx] = Value::U32(u32::MAX);
            self.out.asked.push(u32::try_from(idx).unwrap_or(u32::MAX));
            return Ok(());
        };
        if let Some(SymbolId(id)) = dict.get(name) {
            self.out.rows[idx] = Value::U32(id);
            return Ok(());
        }
        // values new to the dictionary in this message count against the cap together
        let set = &mut self.new_names[c];
        if !set.contains(name) {
            if dict.len() + set.len() >= self.max_dict_entries {
                return Err(conv("dictionary is full (max_dict_entries)"));
            }
            set.insert(name.into());
        }
        // interned by `finish_row`, once the row has been accepted
        self.out.pending[idx].clear();
        self.out.pending[idx].push_str(name);
        self.out.pending_set[idx] = true;
        self.out.rows[idx] = Value::U32(u32::MAX);
        Ok(())
    }
}

/// What one message mapped to: its rows (row-major, `n_rows × columns`), why a row was refused,
/// and the dictionary values not interned yet. Produced by [`RowMapper::parse`]; a mapping worker
/// that has no access to the dictionaries hands it over ([`RowMapper::swap_mapped`]) and whoever
/// owns them calls [`Self::resolve`], then [`Self::parsed`] and [`Self::finish_row`] per row.
/// Buffers are reused across messages.
#[derive(Debug, Clone)]
pub struct Mapped {
    schema: Schema,
    partition_col: Option<usize>,
    rows: Vec<Value>,
    n_rows: usize,
    row_err: Vec<Option<MapError>>,
    pending: Vec<String>,
    pending_set: Vec<bool>,
    /// Scratch of [`Self::resolve`]: new values per column in the message.
    new_names: Vec<std::collections::HashSet<Box<str>>>,
    /// Cells left pending by a mapper without the dictionaries: once resolved, their ids are
    /// what that mapper learns ([`RowMapper::learn`]).
    asked: Vec<u32>,
}

impl Mapped {
    /// An empty result for rows of `schema`.
    #[must_use]
    pub fn new(schema: Schema, partition_col: Option<usize>) -> Self {
        Self {
            schema,
            partition_col,
            rows: Vec::new(),
            n_rows: 0,
            row_err: Vec::new(),
            pending: Vec::new(),
            pending_set: Vec::new(),
            new_names: Vec::new(),
            asked: Vec::new(),
        }
    }

    /// Rows of the message.
    #[must_use]
    pub fn n_rows(&self) -> usize {
        self.n_rows
    }

    /// Resolve pending dictionary values against `dicts` without adding to them: known values
    /// become their ids; new ones stay pending and count against `max_entries` together, per
    /// column, in row order, exactly as mapping with the dictionaries at hand counts them (a row
    /// that would exceed the cap is refused at that column; values of the columns before it
    /// still count). Needed only for a result mapped without the dictionaries.
    pub fn resolve(&mut self, dicts: &[Option<SymbolDict>], max_entries: usize) {
        let ncols = self.schema.fields().len();
        if self.new_names.len() < ncols {
            self.new_names.resize_with(ncols, Default::default);
        }
        for set in &mut self.new_names {
            if !set.is_empty() {
                set.clear();
            }
        }
        for r in 0..self.n_rows {
            let base = r * ncols;
            for c in 0..ncols {
                let idx = base + c;
                if !self.pending_set[idx] {
                    continue;
                }
                let Some(dict) = dicts.get(c).and_then(Option::as_ref) else {
                    continue;
                };
                let name = self.pending[idx].as_str();
                if let Some(SymbolId(id)) = dict.get(name) {
                    self.rows[idx] = Value::U32(id);
                    self.pending_set[idx] = false;
                    continue;
                }
                let set = &mut self.new_names[c];
                if !set.contains(name) {
                    if dict.len() + set.len() >= max_entries {
                        self.row_err[r] = Some(MapError::Convert {
                            column: self.schema.fields()[c].name.clone(),
                            reason: "dictionary is full (max_dict_entries)".to_owned(),
                        });
                        break;
                    }
                    set.insert(name.into());
                }
            }
        }
    }

    /// Row `r`: its timestamp and partition key, or why it was refused.
    pub fn parsed(&self, r: usize) -> Result<Parsed, MapError> {
        if let Some(e) = self.row_err.get(r).and_then(Option::as_ref) {
            return Err(e.clone());
        }
        let ncols = self.schema.fields().len();
        let base = r * ncols;
        let ts = match self.rows.get(base) {
            Some(Value::I64(t)) => *t,
            _ => 0,
        };
        let key = match self.partition_col {
            None => Some(0),
            Some(i) if self.pending_set[base + i] => None,
            Some(i) => match self.rows[base + i] {
                Value::U32(k) => Some(k),
                _ => Some(0),
            },
        };
        Ok(Parsed { ts, key })
    }

    /// Take row `r`: add its new dictionary values and return its partition key and values.
    /// Call it only once the row is going to be stored.
    pub fn finish_row(&mut self, r: usize, dicts: &mut [Option<SymbolDict>]) -> (u32, &[Value]) {
        let ncols = self.schema.fields().len();
        let base = r * ncols;
        for c in 0..ncols {
            if std::mem::take(&mut self.pending_set[base + c])
                && let Some(d) = dicts.get_mut(c).and_then(Option::as_mut)
            {
                let SymbolId(id) = d.intern(&self.pending[base + c]);
                self.rows[base + c] = Value::U32(id);
            }
        }
        let key = match self.partition_col {
            Some(i) => match self.rows[base + i] {
                Value::U32(k) => k,
                _ => 0,
            },
            None => 0,
        };
        (key, &self.rows[base..base + ncols])
    }
}

/// Most dictionary values a mapping worker remembers per column ([`RowMapper::learn`]); values
/// beyond them are resolved by the dictionaries' owner, as before.
pub const MAX_LEARNED_PER_COLUMN: usize = 1_000_000;

/// Longest value accepted for a `dict` column, in bytes.
pub const MAX_DICT_VALUE_BYTES: usize = 256;

/// Default cap on distinct values per `dict` column (each value of a `partition_by` column is
/// one partition).
pub const DEFAULT_MAX_DICT_ENTRIES: usize = 1_000_000;

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

/// The name of metric `m` and the metric with its datatype and value as its BIRTH defined them
/// (`None`: transient, not stored). A BIRTH records the definition; a DATA metric is looked up
/// by alias or by name, and keeps the datatype it carries when it carries one.
fn sp_definition<'a>(
    defs: &mut Defs<'a>,
    t: &sparkplug::Topic<'a>,
    m: sparkplug::Metric<'a>,
) -> Result<Option<(&'a str, sparkplug::Metric<'a>)>, MapError> {
    let name = match m.name {
        Some(n) => Some(
            std::str::from_utf8(n).map_err(|_| MapError::Malformed("metric name is not utf-8"))?,
        ),
        None => None,
    };
    let (name, ty, transient) = match (defs, name, m.alias) {
        (Defs::Birth(d), Some(n), alias) => {
            // a full table only loses the definition: this row is still stored
            d.define(
                t.group,
                t.node,
                t.device,
                alias,
                n,
                m.datatype,
                m.is_transient,
            );
            (n, m.datatype, m.is_transient)
        }
        (Defs::Birth(_), None, _) => {
            return Err(MapError::Malformed("BIRTH metric without a name"));
        }
        (Defs::Data(node), Some(n), _) => {
            let (ty, tr) = node
                .and_then(|nd| nd.by_name(t.device, n))
                .unwrap_or((0, false));
            (n, ty, tr)
        }
        (Defs::Data(node), None, Some(a)) => {
            let d = node
                .and_then(|nd| nd.by_alias(a))
                .ok_or(MapError::Malformed(
                    "metric alias without a BIRTH that defines it",
                ))?;
            (d.name, d.datatype, d.transient)
        }
        (Defs::Data(_), None, None) => {
            return Err(MapError::Malformed("metric without a name or an alias"));
        }
    };
    if m.is_transient || transient {
        return Ok(None);
    }
    let m = if m.datatype == 0 && ty != 0 {
        sparkplug::Metric {
            datatype: ty,
            value: sparkplug::retype(m.value, ty),
            ..m
        }
    } else {
        m
    };
    Ok(Some((name, m)))
}

/// The definitions one message works with: a BIRTH records into the table, a DATA message reads
/// its node's definitions, looked up once for all its metrics.
enum Defs<'a> {
    Birth(&'a mut Definitions),
    Data(Option<sparkplug::NodeDefs<'a>>),
}

/// What a Sparkplug selector reads from metric `m` (named `name`) of a message.
fn sp_raw<'a>(
    f: SpField,
    m: &sparkplug::Metric<'a>,
    name: &'a str,
    h: &sparkplug::Header,
    t: &sparkplug::Topic<'a>,
) -> Result<Option<SpRaw<'a>>, MapError> {
    use sparkplug::Value as V;
    Ok(match f {
        SpField::Metric => Some(SpRaw::Text(name)),
        SpField::Value | SpField::String => match m.value {
            V::Str(s) => {
                Some(SpRaw::Text(std::str::from_utf8(s).map_err(|_| {
                    MapError::Malformed("string value is not utf-8")
                })?))
            }
            _ if f == SpField::String => None,
            V::Int(v) => Some(SpRaw::Int(v)),
            V::UInt(v) if m.datatype == sparkplug::datatype::DATETIME => Some(SpRaw::Ms(v)),
            V::UInt(v) => Some(SpRaw::UInt(v)),
            V::Float(x) => Some(SpRaw::Float(x)),
            V::Bool(b) => Some(SpRaw::Bool(b)),
            V::Null => None,
            V::Unsupported => {
                return Err(MapError::Malformed(
                    "metric datatype has no scalar value (DataSet, Template, Bytes, File or array)",
                ));
            }
        },
        SpField::Timestamp => m.timestamp.or(h.timestamp).map(SpRaw::Ms),
        SpField::Datatype => Some(SpRaw::UInt(u64::from(m.datatype))),
        SpField::Historical => Some(SpRaw::Bool(m.is_historical)),
        SpField::Seq => h.seq.map(SpRaw::UInt),
        SpField::Group => Some(SpRaw::Text(t.group)),
        SpField::Node => Some(SpRaw::Text(t.node)),
        SpField::Device => (!t.device.is_empty()).then_some(SpRaw::Text(t.device)),
        SpField::Type => Some(SpRaw::Text(t.kind)),
    })
}

/// A Sparkplug value as a value of a non-dictionary column type, or why it does not fit.
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
fn sp_typed(ty: ColumnType, raw: SpRaw<'_>) -> Result<Value, &'static str> {
    // i64 range as f64: [-2^63, 2^63)
    const LIMIT: f64 = 9_223_372_036_854_775_808.0;
    let int = |raw: SpRaw<'_>| -> Option<i64> {
        match raw {
            SpRaw::Int(v) => Some(v),
            SpRaw::UInt(v) | SpRaw::Ms(v) => i64::try_from(v).ok(),
            SpRaw::Bool(b) => Some(i64::from(b)),
            SpRaw::Float(x) if x.fract() == 0.0 && (-LIMIT..LIMIT).contains(&x) => Some(x as i64),
            SpRaw::Float(_) => None,
            SpRaw::Text(s) => s.trim().parse().ok(),
        }
    };
    Ok(match ty {
        ColumnType::TimestampNs => Value::I64(match raw {
            SpRaw::Ms(v) | SpRaw::UInt(v) => i64::try_from(v)
                .ok()
                .and_then(|v| v.checked_mul(1_000_000))
                .ok_or("timestamp out of range")?,
            SpRaw::Int(v) => v.checked_mul(1_000_000).ok_or("timestamp out of range")?,
            SpRaw::Text(s) => parse_rfc3339_ns(s.trim()).ok_or("bad timestamp")?,
            SpRaw::Float(_) | SpRaw::Bool(_) => return Err("expected a timestamp"),
        }),
        ColumnType::FixedPoint { scale } => {
            let pow = 10i64
                .checked_pow(u32::from(scale))
                .ok_or("scale too large")?;
            Value::I64(match raw {
                SpRaw::Float(x) => {
                    let y = (x * pow as f64).round();
                    if !y.is_finite() || !(-LIMIT..LIMIT).contains(&y) {
                        return Err("number out of range");
                    }
                    y as i64
                }
                SpRaw::Text(s) => {
                    parse_fixed(s.trim().as_bytes(), scale).map_err(|_| "expected a decimal")?
                }
                other => int(other)
                    .and_then(|v| v.checked_mul(pow))
                    .ok_or("number out of range")?,
            })
        }
        ColumnType::Int64 => Value::I64(int(raw).ok_or("expected an integer")?),
        ColumnType::U8 => Value::U8(
            int(raw)
                .and_then(|v| u8::try_from(v).ok())
                .ok_or("expected 0..255")?,
        ),
        ColumnType::Float64 => {
            let x = match raw {
                SpRaw::Float(x) => x,
                SpRaw::Int(v) => v as f64,
                SpRaw::UInt(v) | SpRaw::Ms(v) => v as f64,
                SpRaw::Bool(b) => f64::from(u8::from(b)),
                SpRaw::Text(s) => s.trim().parse().map_err(|_| "expected a number")?,
            };
            if !x.is_finite() {
                return Err("expected a finite number");
            }
            Value::F64(x)
        }
        ColumnType::DictU32 => return Err("not a scalar column"),
    })
}

/// A raw value as a value of a non-dictionary column type, or why it does not fit.
fn typed_value(
    ty: ColumnType,
    raw: Scalar<'_>,
    text: Option<&str>,
    num: Option<&[u8]>,
    kind: Option<json_scan::Kind>,
    unit: TsUnit,
) -> Result<Value, std::borrow::Cow<'static, str>> {
    use json_scan::Kind as K;
    Ok(match ty {
        ColumnType::TimestampNs => Value::I64(parse_ts(text, num, unit).ok_or("bad timestamp")?),
        ColumnType::FixedPoint { scale } => Value::I64(match (text, num) {
            (Some(s), _) => parse_fixed(s.trim().as_bytes(), scale).map_err(|e| e.to_string())?,
            (None, Some(n)) => fixed_from_number(n, scale).ok_or("number out of range")?,
            _ => return Err("expected a decimal".into()),
        }),
        ColumnType::Int64 => Value::I64(match (text, num) {
            (Some(s), _) => s.trim().parse().map_err(|_| "expected an integer")?,
            (None, Some(n)) => ascii(n).parse().map_err(|_| "expected an integer")?,
            _ => return Err("expected an integer".into()),
        }),
        ColumnType::U8 => Value::U8(match (raw, num, kind) {
            (Scalar::Text(s), _, _) => s.parse().map_err(|_| "expected 0..255")?,
            (_, Some(n), _) => ascii(n)
                .parse::<u64>()
                .ok()
                .and_then(|v| u8::try_from(v).ok())
                .ok_or("expected 0..255")?,
            (_, _, Some(K::True)) => 1,
            (_, _, Some(K::False)) => 0,
            // JSON strings are not accepted for a byte column
            _ => return Err("expected 0..255".into()),
        }),
        ColumnType::Float64 => {
            let x: f64 = match (text, num) {
                (Some(s), _) => s.trim().parse().map_err(|_| "expected a number")?,
                (None, Some(n)) => ascii(n).parse().map_err(|_| "expected a number")?,
                _ => return Err("expected a number".into()),
            };
            if !x.is_finite() {
                return Err("expected a finite number".into());
            }
            Value::F64(x)
        }
        ColumnType::DictU32 => return Err("not a scalar column".into()),
    })
}

/// The bytes of a validated JSON number (ASCII by the grammar) as text.
#[inline]
fn ascii(n: &[u8]) -> &str {
    std::str::from_utf8(n).unwrap_or("")
}

/// `$topic` (`None`) or its `n`-th `/` segment.
fn topic_segment(topic: &str, n: Option<usize>) -> Option<&str> {
    match n {
        None => Some(topic),
        Some(n) => topic.split('/').nth(n),
    }
}

/// A JSON number used as a dictionary value, in canonical form: integers as integers (`-0` is
/// `0`), other numbers as the shortest text that reads back as the same `f64`.
fn canonical_number<'b>(n: &[u8], buf: &'b mut StackStr) -> Option<&'b str> {
    use std::fmt::Write as _;
    let t = ascii(n);
    if let Ok(i) = t.parse::<i64>() {
        write!(buf, "{i}").ok()?;
    } else if let Ok(u) = t.parse::<u64>() {
        write!(buf, "{u}").ok()?;
    } else {
        write!(buf, "{}", t.parse::<f64>().ok()?).ok()?;
    }
    Some(buf.as_str())
}

/// A JSON number (its text) as a fixed-point integer with `scale` decimals. Exact when the
/// text has at most `scale` decimals and no exponent; other numbers (float artefacts such as
/// `21.700000000000003`, `1e-3`) are rounded to the nearest representable value. `None` when the
/// value does not fit an `i64`.
fn fixed_from_number(n: &[u8], scale: u8) -> Option<i64> {
    let has_exp = n.iter().any(|&c| c == b'e' || c == b'E');
    if !has_exp {
        if let Ok(v) = parse_fixed(n, scale) {
            return Some(v);
        }
        if !n.contains(&b'.') {
            return None; // an integer that does not fit
        }
    }
    let scaled = (ascii(n).parse::<f64>().ok()? * 10f64.powi(i32::from(scale))).round();
    // 2^63 is exactly representable; anything at or beyond it does not fit
    #[allow(clippy::cast_possible_truncation)]
    (scaled.is_finite() && scaled.abs() < 9_223_372_036_854_775_808.0).then_some(scaled as i64)
}

/// A timestamp from a string (`text`) or a JSON number (`num`) in `unit`.
fn parse_ts(text: Option<&str>, num: Option<&[u8]>, unit: TsUnit) -> Option<i64> {
    let scale = match unit {
        TsUnit::Nanos => 0,
        TsUnit::Micros => 3,
        TsUnit::Millis => 6,
        TsUnit::Seconds => 9,
        TsUnit::Rfc3339 => return parse_rfc3339_ns(text?.trim()),
    };
    match (text, num) {
        (Some(s), _) => parse_fixed(s.trim().as_bytes(), scale).ok(),
        (None, Some(n)) => fixed_from_number(n, scale),
        _ => None,
    }
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

    fn rows_schema() -> Schema {
        Schema::new(vec![
            Field::new("ts", ColumnType::TimestampNs),
            Field::new("dev", ColumnType::DictU32),
            Field::nullable("temp", ColumnType::Float64),
            Field::new("site", ColumnType::DictU32),
        ])
        .unwrap()
    }

    fn rows_mapper(map: &[(&str, &str)]) -> RowMapper {
        let map: HashMap<String, String> = map
            .iter()
            .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
            .collect();
        RowMapper::new(
            rows_schema(),
            &map,
            false,
            TsUnit::Millis,
            false,
            Some("dev"),
        )
        .unwrap()
        .with_rows(Some("samples"))
        .unwrap()
    }

    #[test]
    fn rows_one_per_array_element() {
        let mut m = rows_mapper(&[
            ("ts", "t"),
            ("dev", "$msg.dev"),
            ("temp", "v"),
            ("site", "$msg.meta.site"),
        ]);
        let mut d = RowMapper::dictionaries_for(&rows_schema());
        // message fields before and after the array; a missing value is NULL in its row only
        let msg = br#"{"dev":"m1","samples":[{"t":1000,"v":20.5},{"t":1001},{"t":1002,"v":null}],"meta":{"site":"p1"}}"#;
        assert_eq!(m.parse(msg, 0, false, &d).unwrap(), 3);
        for r in 0..3 {
            let p = m.parsed(r).unwrap();
            assert_eq!(p.ts, (1000 + i64::try_from(r).unwrap()) * 1_000_000);
            let (key, row) = m.finish_row(r, &mut d);
            assert_eq!(key, 0);
            assert_eq!(row[1], Value::U32(0));
            assert_eq!(row[3], Value::U32(0));
            assert_eq!(
                row[2],
                if r == 0 {
                    Value::F64(20.5)
                } else {
                    Value::Null
                }
            );
        }
        assert_eq!(d[1].as_ref().unwrap().name(SymbolId(0)), Some("m1"));
        // empty array: no row, not an error; no array: the message is refused
        assert_eq!(
            m.parse(br#"{"dev":"m1","samples":[]}"#, 0, false, &d)
                .unwrap(),
            0
        );
        assert!(matches!(
            m.parse(br#"{"dev":"m1"}"#, 0, false, &d),
            Err(MapError::Missing(_))
        ));
    }

    #[test]
    fn a_bad_element_refuses_only_its_row() {
        let mut m = rows_mapper(&[
            ("ts", "t"),
            ("dev", "d"),
            ("temp", "v"),
            ("site", "$msg.site"),
        ]);
        let mut d = RowMapper::dictionaries_for(&rows_schema());
        // each element names its own device; the second has a bad timestamp
        let msg = br#"{"site":"s","samples":[{"t":1,"d":"a","v":1},{"t":"x","d":"b","v":2},{"t":3,"d":"c","v":3}]}"#;
        assert_eq!(m.parse(msg, 0, false, &d).unwrap(), 3);
        assert!(
            m.parsed(0).unwrap().key.is_none(),
            "a new device: no key yet"
        );
        assert!(m.parsed(1).is_err());
        let (k0, _) = m.finish_row(0, &mut d);
        let (k2, row2) = m.finish_row(2, &mut d);
        assert_eq!((k0, k2), (0, 1), "the refused row added no device");
        assert_eq!(row2[2], Value::F64(3.0));
        assert_eq!(d[1].as_ref().unwrap().len(), 2);
    }

    /// One message of many rows cannot overshoot the dictionary cap: new values of the message
    /// count against it together (a repeated new value once).
    #[test]
    fn rows_respect_the_dictionary_cap_within_one_message() {
        let mut m = rows_mapper(&[
            ("ts", "t"),
            ("dev", "d"),
            ("temp", "v"),
            ("site", "$msg.site"),
        ])
        .with_max_dict_entries(4);
        let mut d = RowMapper::dictionaries_for(&rows_schema());
        let elems: Vec<String> = (0..10)
            .map(|i| {
                format!(
                    r#"{{"t":{i},"d":"dev{}","v":1}}"#,
                    if i == 1 { 0 } else { i }
                )
            })
            .collect();
        let msg = format!(r#"{{"site":"s","samples":[{}]}}"#, elems.join(","));
        let n = m.parse(msg.as_bytes(), 0, false, &d).unwrap();
        assert_eq!(n, 10);
        let mut stored = 0;
        for r in 0..n {
            if m.parsed(r).is_ok() {
                m.finish_row(r, &mut d);
                stored += 1;
            }
        }
        // site "s" takes one entry of its own dictionary; devices: dev0 (twice), dev2, dev3, dev4
        assert_eq!(stored, 5, "dev0 twice plus three more new devices");
        assert_eq!(d[1].as_ref().unwrap().len(), 4);
    }

    #[test]
    fn rows_needs_json_and_a_path() {
        let schema = rows_schema();
        let map: HashMap<String, String> =
            [("ts", "0"), ("dev", "1"), ("temp", "2"), ("site", "3")]
                .into_iter()
                .map(|(a, b)| (a.to_owned(), b.to_owned()))
                .collect();
        let csv = RowMapper::new(schema.clone(), &map, true, TsUnit::Millis, false, None).unwrap();
        assert!(csv.with_rows(Some("samples")).is_err());
        let json =
            RowMapper::new(schema, &HashMap::new(), false, TsUnit::Millis, true, None).unwrap();
        assert!(json.with_rows(Some("a..b")).is_err());
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
        let n = m
            .parse(br#"{"s":"ghost-3","t":1,"n":1,"ok":1,"l":2}"#, 7, false, &d)
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(m.parsed(0).unwrap(), Parsed { ts: 7, key: None });
        assert_eq!(d[1].as_ref().unwrap().len(), 0);
        // accepted: interned by finish_row, and known on the next parse
        let (key, row) = m.finish_row(0, &mut d);
        assert_eq!((key, row[1]), (0, Value::U32(0)));
        m.parse(br#"{"s":"ghost-3","t":1,"n":1,"ok":1,"l":2}"#, 8, false, &d)
            .unwrap();
        assert_eq!(
            m.parsed(0).unwrap(),
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
