//! Sparkplug B payloads (Eclipse Sparkplug 3.0, the MQTT payload of most industrial gateways and
//! SCADA systems), decoded without generated code and without allocation.
//!
//! A payload is a protobuf message: a timestamp, a sequence number and a list of metrics, each
//! with a name (or only an alias after the BIRTH that defined it), a datatype, a timestamp and one
//! value. [`for_each_metric`] walks the bytes once and hands every metric to a callback as a
//! [`Metric`] that borrows the payload: names and string values are slices, numbers are decoded
//! from their varint or fixed-width encodings in place.
//!
//! Supported values: the scalar datatypes (integers of every width and sign, `Float`, `Double`,
//! `Boolean`, `String`/`Text`/`UUID`, `DateTime`) and `is_null`. `DataSet`, `Template`, `Bytes`,
//! `File`, property sets and arrays are reported as [`Value::Unsupported`] (the caller skips them).
//! Unknown fields are skipped by their wire type, as protobuf requires.
//!
//! [`Definitions`] keeps what BIRTH messages define, per edge node: each metric's datatype and
//! transient flag, and the aliases DATA messages use instead of names. It serialises, so that a
//! server can write it into its log and read DATA messages after a restart without waiting for
//! the next BIRTH.

use std::collections::HashMap;

/// Most metrics one payload may carry: bounds the work of one message.
pub const MAX_METRICS: usize = 65_536;

/// A decode failure: the payload is not a Sparkplug B protobuf message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeError(pub &'static str);

/// Sparkplug datatype codes (the subset with scalar values).
pub mod datatype {
    /// `Int8`.
    pub const INT8: u32 = 1;
    /// `Int16`.
    pub const INT16: u32 = 2;
    /// `Int32`.
    pub const INT32: u32 = 3;
    /// `Int64`.
    pub const INT64: u32 = 4;
    /// `UInt8`.
    pub const UINT8: u32 = 5;
    /// `UInt16`.
    pub const UINT16: u32 = 6;
    /// `UInt32`.
    pub const UINT32: u32 = 7;
    /// `UInt64`.
    pub const UINT64: u32 = 8;
    /// `Float`.
    pub const FLOAT: u32 = 9;
    /// `Double`.
    pub const DOUBLE: u32 = 10;
    /// `Boolean`.
    pub const BOOLEAN: u32 = 11;
    /// `String`.
    pub const STRING: u32 = 12;
    /// `DateTime` (ms since the epoch, in `long_value`).
    pub const DATETIME: u32 = 13;
    /// `Text`.
    pub const TEXT: u32 = 14;
    /// `UUID` (as a string).
    pub const UUID: u32 = 15;
}

/// The value of one metric.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value<'a> {
    /// A signed integer (any width, sign-extended as the datatype says).
    Int(i64),
    /// An unsigned integer.
    UInt(u64),
    /// `Float` (widened) or `Double`.
    Float(f64),
    /// `Boolean`.
    Bool(bool),
    /// `String`, `Text` or `UUID` (UTF-8 is checked by the caller when it uses the text).
    Str(&'a [u8]),
    /// `is_null` was set, or no value field was present.
    Null,
    /// A datatype with no scalar value (`DataSet`, `Template`, `Bytes`, …).
    Unsupported,
}

/// One metric of a payload, borrowing the payload bytes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Metric<'a> {
    /// Name, when the message carries it (BIRTH always does; DATA may send only the alias).
    pub name: Option<&'a [u8]>,
    /// Alias, when used.
    pub alias: Option<u64>,
    /// Metric timestamp, ms since the epoch.
    pub timestamp: Option<u64>,
    /// Datatype code ([`datatype`]); 0 when absent.
    pub datatype: u32,
    /// Historical value (sent late, from the edge node's buffer).
    pub is_historical: bool,
    /// Transient value: the specification says it is not to be historised.
    pub is_transient: bool,
    /// The value.
    pub value: Value<'a>,
}

/// Payload-level fields.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Header {
    /// Payload timestamp, ms since the epoch.
    pub timestamp: Option<u64>,
    /// Sequence number (0–255).
    pub seq: Option<u64>,
    /// Number of metrics.
    pub metrics: usize,
}

struct Reader<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Reader<'a> {
    #[inline]
    fn done(&self) -> bool {
        self.i >= self.b.len()
    }

    /// A base-128 varint (at most 10 bytes). Tags and short lengths are one byte: that case is
    /// one load and one compare. With 10 bytes left the decode runs over a fixed-size window,
    /// so the compiler drops the per-byte bounds checks and unrolls the loop (timestamps in
    /// milliseconds take 6 bytes); only the last bytes of a payload take the checked loop.
    #[inline]
    fn varint(&mut self) -> Result<u64, DecodeError> {
        let i = self.i;
        if let Some(&x) = self.b.get(i)
            && x < 0x80
        {
            self.i = i + 1;
            return Ok(u64::from(x));
        }
        if let Some(w) = self.b.get(i..i + 10) {
            let w: &[u8; 10] = w.try_into().map_err(|_| DecodeError("truncated varint"))?;
            let mut v = 0u64;
            for (k, &byte) in w.iter().enumerate() {
                v |= u64::from(byte & 0x7f) << (7 * k);
                if byte & 0x80 == 0 {
                    if k == 9 && byte > 1 {
                        return Err(DecodeError("varint overflows 64 bits"));
                    }
                    self.i = i + k + 1;
                    return Ok(v);
                }
            }
            return Err(DecodeError("varint longer than 10 bytes"));
        }
        self.varint_tail()
    }

    /// [`Self::varint`] near the end of the payload, with a bounds check per byte.
    #[cold]
    fn varint_tail(&mut self) -> Result<u64, DecodeError> {
        let mut v = 0u64;
        for shift in (0..70).step_by(7) {
            let byte = *self.b.get(self.i).ok_or(DecodeError("truncated varint"))?;
            self.i += 1;
            if shift == 63 && byte > 1 {
                return Err(DecodeError("varint overflows 64 bits"));
            }
            v |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err(DecodeError("varint longer than 10 bytes"))
    }

    #[inline]
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let s = self
            .b
            .get(self.i..self.i + N)
            .ok_or(DecodeError("truncated fixed-width field"))?;
        self.i += N;
        let mut a = [0u8; N];
        a.copy_from_slice(s);
        Ok(a)
    }

    #[inline]
    fn bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let len = usize::try_from(self.varint()?).map_err(|_| DecodeError("length too large"))?;
        let end = self
            .i
            .checked_add(len)
            .filter(|&e| e <= self.b.len())
            .ok_or(DecodeError("truncated length-delimited field"))?;
        let s = &self.b[self.i..end];
        self.i = end;
        Ok(s)
    }

    /// Skip a field of `wire` type.
    fn skip(&mut self, wire: u64) -> Result<(), DecodeError> {
        match wire {
            0 => self.varint().map(|_| ()),
            1 => self.fixed::<8>().map(|_| ()),
            2 => self.bytes().map(|_| ()),
            5 => self.fixed::<4>().map(|_| ()),
            _ => Err(DecodeError("unsupported wire type")),
        }
    }

    #[inline]
    fn tag(&mut self) -> Result<(u64, u64), DecodeError> {
        let t = self.varint()?;
        let field = t >> 3;
        if field == 0 {
            return Err(DecodeError("field number 0"));
        }
        Ok((field, t & 7))
    }
}

/// Decode `payload`, calling `on_metric` for every metric in order. Returns the payload-level
/// fields (which a protobuf encoder may write after the metrics, so they are only complete at the
/// end).
pub fn for_each_metric<'a>(
    payload: &'a [u8],
    mut on_metric: impl FnMut(Metric<'a>),
) -> Result<Header, DecodeError> {
    let mut r = Reader { b: payload, i: 0 };
    let mut h = Header::default();
    let mut n = 0usize;
    while !r.done() {
        let (field, wire) = r.tag()?;
        match (field, wire) {
            (1, 0) => h.timestamp = Some(r.varint()?),
            (2, 2) => {
                n += 1;
                if n > MAX_METRICS {
                    return Err(DecodeError("too many metrics in one payload"));
                }
                on_metric(metric(r.bytes()?)?);
            }
            (3, 0) => h.seq = Some(r.varint()?),
            // 4 uuid, 5 body, extensions: not used
            _ => r.skip(wire)?,
        }
    }
    h.metrics = n;
    Ok(h)
}

/// The payload-level fields and the number of metrics, without decoding the metrics (a protobuf
/// encoder writes `seq`, field 3, after the metrics, field 2: the fields a row needs from the
/// payload are only known once the whole message has been walked).
pub fn header(payload: &[u8]) -> Result<Header, DecodeError> {
    let mut r = Reader { b: payload, i: 0 };
    let mut h = Header::default();
    while !r.done() {
        let (field, wire) = r.tag()?;
        match (field, wire) {
            (1, 0) => h.timestamp = Some(r.varint()?),
            (2, 2) => {
                h.metrics += 1;
                if h.metrics > MAX_METRICS {
                    return Err(DecodeError("too many metrics in one payload"));
                }
                r.bytes()?;
            }
            (3, 0) => h.seq = Some(r.varint()?),
            _ => r.skip(wire)?,
        }
    }
    Ok(h)
}

/// The raw value field of a metric, interpreted once the datatype is known (it may come
/// first on the wire).
enum RawVal<'a> {
    None,
    Int(u64),
    Long(u64),
    Float(f32),
    Double(f64),
    Bool(bool),
    Str(&'a [u8]),
    Other,
}

/// Decode one `Metric` message.
fn metric(b: &[u8]) -> Result<Metric<'_>, DecodeError> {
    use datatype as dt;
    let mut r = Reader { b, i: 0 };
    let mut m = Metric {
        name: None,
        alias: None,
        timestamp: None,
        datatype: 0,
        is_historical: false,
        is_transient: false,
        value: Value::Null,
    };
    let mut is_null = false;
    let mut raw = RawVal::None;
    while !r.done() {
        let (field, wire) = r.tag()?;
        match (field, wire) {
            (1, 2) => m.name = Some(r.bytes()?),
            (2, 0) => m.alias = Some(r.varint()?),
            (3, 0) => m.timestamp = Some(r.varint()?),
            (4, 0) => m.datatype = u32::try_from(r.varint()?).unwrap_or(u32::MAX),
            (5, 0) => m.is_historical = r.varint()? != 0,
            (6, 0) => m.is_transient = r.varint()? != 0,
            (7, 0) => is_null = r.varint()? != 0,
            (10, 0) => raw = RawVal::Int(r.varint()?),
            (11, 0) => raw = RawVal::Long(r.varint()?),
            (12, 5) => raw = RawVal::Float(f32::from_le_bytes(r.fixed::<4>()?)),
            (13, 1) => raw = RawVal::Double(f64::from_le_bytes(r.fixed::<8>()?)),
            (14, 0) => raw = RawVal::Bool(r.varint()? != 0),
            (15, 2) => raw = RawVal::Str(r.bytes()?),
            (16..=19, 2) => {
                r.bytes()?;
                raw = RawVal::Other;
            }
            _ => r.skip(wire)?,
        }
    }
    // integers are carried as unsigned protobuf fields holding the two's complement of the
    // value at the datatype's width (Sparkplug 3.0, §6.4.16)
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let value = match (raw, m.datatype) {
        _ if is_null => Value::Null,
        (RawVal::None, _) => Value::Null,
        (RawVal::Int(v) | RawVal::Long(v), dt::INT8) => Value::Int(i64::from(v as u8 as i8)),
        (RawVal::Int(v) | RawVal::Long(v), dt::INT16) => Value::Int(i64::from(v as u16 as i16)),
        (RawVal::Int(v) | RawVal::Long(v), dt::INT32) => Value::Int(i64::from(v as u32 as i32)),
        (RawVal::Int(v) | RawVal::Long(v), dt::INT64 | dt::DATETIME) => Value::Int(v as i64),
        (RawVal::Int(v) | RawVal::Long(v), dt::UINT8) => Value::UInt(v & 0xff),
        (RawVal::Int(v) | RawVal::Long(v), dt::UINT16) => Value::UInt(v & 0xffff),
        (RawVal::Int(v) | RawVal::Long(v), dt::UINT32) => Value::UInt(v & 0xffff_ffff),
        // UInt64, or an integer field without a usable datatype: taken as unsigned
        (RawVal::Int(v) | RawVal::Long(v), _) => Value::UInt(v),
        (RawVal::Float(f), _) => Value::Float(f64::from(f)),
        (RawVal::Double(f), _) => Value::Float(f),
        (RawVal::Bool(b), _) => Value::Bool(b),
        (RawVal::Str(s), dt::STRING | dt::TEXT | dt::UUID | 0) => Value::Str(s),
        (RawVal::Str(_) | RawVal::Other, _) => Value::Unsupported,
    };
    m.value = value;
    Ok(m)
}

/// The parts of a Sparkplug topic `spBv1.0/<group>/<type>/<edge node>[/<device>]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Topic<'a> {
    /// Group id.
    pub group: &'a str,
    /// Message type (`NBIRTH`, `NDATA`, `DBIRTH`, `DDATA`, `NDEATH`, `DDEATH`, `NCMD`, …).
    pub kind: &'a str,
    /// Edge node id.
    pub node: &'a str,
    /// Device id (`""` for node-level messages).
    pub device: &'a str,
}

impl<'a> Topic<'a> {
    /// Parse a Sparkplug B topic; `None` when it is not one.
    #[must_use]
    pub fn parse(topic: &'a str) -> Option<Self> {
        let mut it = topic.split('/');
        if it.next()? != "spBv1.0" {
            return None;
        }
        let group = it.next()?;
        let kind = it.next()?;
        let node = it.next()?;
        let device = it.next().unwrap_or("");
        if it.next().is_some() || group.is_empty() || node.is_empty() {
            return None;
        }
        Some(Self {
            group,
            kind,
            node,
            device,
        })
    }

    /// A BIRTH (defines aliases, carries current values).
    #[must_use]
    pub fn is_birth(&self) -> bool {
        self.kind == "NBIRTH" || self.kind == "DBIRTH"
    }

    /// A message whose metrics are readings (BIRTH or DATA).
    #[must_use]
    pub fn has_readings(&self) -> bool {
        matches!(self.kind, "NBIRTH" | "DBIRTH" | "NDATA" | "DDATA")
    }
}

/// Most definitions one table keeps (all edge nodes and devices of a source together): bounds
/// the memory a publisher can make the server hold.
pub const MAX_DEFINITIONS: usize = 1_000_000;
/// Aliases below this are kept in a vector indexed by the alias (edge nodes number their
/// metrics from 0 up); larger ones in a map.
const DENSE_ALIASES: u64 = 1 << 16;
/// Longest metric, device or node name kept, in bytes.
pub const MAX_NAME_BYTES: usize = 1024;

/// What a BIRTH said about a metric. DATA messages of a compliant edge node leave the datatype
/// and the transient flag out (Sparkplug 3.0), so the host has to remember them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Def<'a> {
    /// Metric name.
    pub name: &'a str,
    /// Datatype code.
    pub datatype: u32,
    /// Not to be historised.
    pub transient: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    name: Box<str>,
    datatype: u32,
    transient: bool,
}

impl Entry {
    fn def(&self) -> Def<'_> {
        Def {
            name: &self.name,
            datatype: self.datatype,
            transient: self.transient,
        }
    }
}

/// Metric name → (datatype, transient), for one device or node.
type Named = HashMap<Box<str>, (u32, bool)>;

/// Definitions of one edge node: by alias (unique across the node and its devices) and by
/// device and name.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Node {
    dense: Vec<Option<Entry>>,
    sparse: HashMap<u64, Entry>,
    /// device (`""` for the node itself) → its metrics
    named: HashMap<Box<str>, Named>,
    len: usize,
}

impl Node {
    fn alias(&self, alias: u64) -> Option<&Entry> {
        if alias < DENSE_ALIASES {
            self.dense.get(alias as usize)?.as_ref()
        } else {
            self.sparse.get(&alias)
        }
    }

    /// Bind an alias; returns whether it is new.
    fn set_alias(&mut self, alias: u64, e: Entry) -> bool {
        if alias < DENSE_ALIASES {
            let i = alias as usize;
            if self.dense.len() <= i {
                self.dense.resize(i + 1, None);
            }
            self.dense[i].replace(e).is_none()
        } else {
            self.sparse.insert(alias, e).is_none()
        }
    }

    fn aliases(&self) -> impl Iterator<Item = (u64, &Entry)> {
        let mut sparse: Vec<_> = self.sparse.iter().map(|(a, e)| (*a, e)).collect();
        sparse.sort_unstable_by_key(|(a, _)| *a);
        self.dense
            .iter()
            .enumerate()
            .filter_map(|(a, e)| Some((a as u64, e.as_ref()?)))
            .chain(sparse)
    }
}

/// Metric definitions from the BIRTH messages of every edge node a source has seen: datatype
/// and transient flag per metric, and the alias bindings that DATA messages rely on. Keyed by
/// group and edge node.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Definitions {
    /// `group \0 node` → definitions.
    nodes: HashMap<Box<str>, Node>,
    total: usize,
    /// Bumped by every change: a writer of snapshots can tell whether one is due.
    generation: u64,
}

/// `group \0 node` on the stack when short (no allocation per lookup).
fn with_key<R>(group: &str, node: &str, f: impl FnOnce(&str) -> R) -> R {
    let mut buf = [0u8; 256];
    let (g, n) = (group.as_bytes(), node.as_bytes());
    let len = g.len() + 1 + n.len();
    if len <= buf.len() {
        buf[..g.len()].copy_from_slice(g);
        buf[g.len() + 1..len].copy_from_slice(n);
        // both parts are `str`s and the separator is ASCII
        f(std::str::from_utf8(&buf[..len]).unwrap_or_default())
    } else {
        f(&format!("{group}\0{node}"))
    }
}

impl Definitions {
    /// An NBIRTH starts a new session of the node: everything it and its devices defined
    /// before no longer holds.
    pub fn reset_node(&mut self, group: &str, node: &str) {
        if let Some(old) = with_key(group, node, |k| self.nodes.remove(k)) {
            self.total -= old.len;
            self.generation += 1;
        }
    }

    /// A DBIRTH redefines the device's metrics (its aliases are node-wide and simply rebound).
    pub fn reset_device(&mut self, group: &str, node: &str, device: &str) {
        let removed = with_key(group, node, |k| {
            let n = self.nodes.get_mut(k)?;
            let gone = n.named.remove(device)?.len();
            n.len -= gone;
            Some(gone)
        });
        if let Some(gone) = removed {
            self.total -= gone;
            self.generation += 1;
        }
    }

    /// Record a BIRTH metric of `device` (`""` for the node): its datatype and transient flag
    /// by name, and its alias when it has one. `false` when the table is full or a name too long
    /// (the metric is then simply not remembered).
    #[allow(clippy::too_many_arguments)]
    pub fn define(
        &mut self,
        group: &str,
        node: &str,
        device: &str,
        alias: Option<u64>,
        name: &str,
        datatype: u32,
        transient: bool,
    ) -> bool {
        if [group, node, device, name]
            .iter()
            .any(|s| s.len() > MAX_NAME_BYTES)
        {
            return false;
        }
        let full = self.total >= MAX_DEFINITIONS;
        let key = format!("{group}\0{node}");
        let n = match self.nodes.get_mut(key.as_str()) {
            Some(n) => n,
            None if full => return false,
            None => self.nodes.entry(key.into()).or_default(),
        };
        let mut added = 0;
        let names = match n.named.get_mut(device) {
            Some(m) => m,
            None if full => return false,
            None => n.named.entry(device.into()).or_default(),
        };
        match names.get_mut(name) {
            Some(d) => *d = (datatype, transient),
            None if full => return false,
            None => {
                names.insert(name.into(), (datatype, transient));
                added += 1;
            }
        }
        if let Some(a) = alias {
            let e = Entry {
                name: name.into(),
                datatype,
                transient,
            };
            if n.alias(a) != Some(&e) {
                if full && n.alias(a).is_none() {
                    return false;
                }
                if n.set_alias(a, e) {
                    added += 1;
                }
            }
        }
        n.len += added;
        self.total += added;
        self.generation += 1;
        true
    }

    /// The definitions of one edge node, looked up once (a DATA message reads all its metrics
    /// through it instead of hashing the node's key per metric).
    #[must_use]
    pub fn node(&self, group: &str, node: &str) -> Option<NodeDefs<'_>> {
        with_key(group, node, |k| self.nodes.get(k)).map(NodeDefs)
    }

    /// The metric bound to `alias` on the node.
    #[must_use]
    pub fn by_alias(&self, group: &str, node: &str, alias: u64) -> Option<Def<'_>> {
        with_key(group, node, |k| self.nodes.get(k))?
            .alias(alias)
            .map(Entry::def)
    }

    /// Datatype and transient flag of the metric `name` of `device` (`""`: the node).
    #[must_use]
    pub fn by_name(
        &self,
        group: &str,
        node: &str,
        device: &str,
        name: &str,
    ) -> Option<(u32, bool)> {
        with_key(group, node, |k| self.nodes.get(k))?
            .named
            .get(device)?
            .get(name)
            .copied()
    }

    /// Number of definitions (names and aliases).
    #[must_use]
    pub fn len(&self) -> usize {
        self.total
    }

    /// Nothing defined.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Changes so far.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Serialise (little-endian, every string with a `u16` length): `u32` nodes; per node its
    /// key, `u32` aliases of (`u64` alias, `u32` datatype, `u8` transient, name), `u32` devices
    /// of (device, `u32` names of (name, `u32` datatype, `u8` transient)). Everything in key
    /// order, so equal tables encode equally.
    pub fn encode(&self, out: &mut Vec<u8>) {
        fn s(out: &mut Vec<u8>, v: &str) {
            out.extend_from_slice(&(v.len() as u16).to_le_bytes());
            out.extend_from_slice(v.as_bytes());
        }
        let mut keys: Vec<&Box<str>> = self.nodes.keys().collect();
        keys.sort_unstable();
        out.extend_from_slice(&(keys.len() as u32).to_le_bytes());
        for k in keys {
            let n = &self.nodes[k];
            s(out, k);
            let aliases: Vec<_> = n.aliases().collect();
            out.extend_from_slice(&(aliases.len() as u32).to_le_bytes());
            for (a, e) in aliases {
                out.extend_from_slice(&a.to_le_bytes());
                out.extend_from_slice(&e.datatype.to_le_bytes());
                out.push(u8::from(e.transient));
                s(out, &e.name);
            }
            let mut devices: Vec<_> = n.named.iter().collect();
            devices.sort_unstable_by(|a, b| a.0.cmp(b.0));
            out.extend_from_slice(&(devices.len() as u32).to_le_bytes());
            for (d, names) in devices {
                s(out, d);
                let mut names: Vec<_> = names.iter().collect();
                names.sort_unstable_by(|a, b| a.0.cmp(b.0));
                out.extend_from_slice(&(names.len() as u32).to_le_bytes());
                for (name, (ty, tr)) in names {
                    s(out, name);
                    out.extend_from_slice(&ty.to_le_bytes());
                    out.push(u8::from(*tr));
                }
            }
        }
    }

    /// Read what [`Self::encode`] wrote; checks every length and the limits.
    pub fn decode(mut b: &[u8]) -> Result<Self, DecodeError> {
        fn take<'a>(b: &mut &'a [u8], n: usize) -> Result<&'a [u8], DecodeError> {
            if b.len() < n {
                return Err(DecodeError("truncated definitions"));
            }
            let (h, t) = b.split_at(n);
            *b = t;
            Ok(h)
        }
        fn int<const N: usize>(b: &mut &[u8]) -> Result<u64, DecodeError> {
            let mut a = [0u8; 8];
            a[..N].copy_from_slice(take(b, N)?);
            Ok(u64::from_le_bytes(a))
        }
        fn text<'a>(b: &mut &'a [u8]) -> Result<&'a str, DecodeError> {
            let n = int::<2>(b)? as usize;
            std::str::from_utf8(take(b, n)?).map_err(|_| DecodeError("definition is not utf-8"))
        }
        let mut t = Self::default();
        let over = DecodeError("definitions over their limits");
        for _ in 0..int::<4>(&mut b)? {
            let key = text(&mut b)?;
            let (group, node) = key
                .split_once('\0')
                .ok_or(DecodeError("definitions key without a node"))?;
            for _ in 0..int::<4>(&mut b)? {
                let alias = int::<8>(&mut b)?;
                let ty = int::<4>(&mut b)? as u32;
                let tr = int::<1>(&mut b)? != 0;
                let name = text(&mut b)?;
                // the name entry is added again below, from the device list
                if !t.define_alias_only(group, node, alias, name, ty, tr) {
                    return Err(over);
                }
            }
            for _ in 0..int::<4>(&mut b)? {
                let device = text(&mut b)?;
                for _ in 0..int::<4>(&mut b)? {
                    let name = text(&mut b)?;
                    let ty = int::<4>(&mut b)? as u32;
                    let tr = int::<1>(&mut b)? != 0;
                    if !t.define(group, node, device, None, name, ty, tr) {
                        return Err(over);
                    }
                }
            }
        }
        if !b.is_empty() {
            return Err(DecodeError("bytes after the definitions"));
        }
        t.generation = 0;
        Ok(t)
    }

    fn define_alias_only(
        &mut self,
        group: &str,
        node: &str,
        alias: u64,
        name: &str,
        datatype: u32,
        transient: bool,
    ) -> bool {
        if self.total >= MAX_DEFINITIONS
            || [group, node, name].iter().any(|s| s.len() > MAX_NAME_BYTES)
        {
            return false;
        }
        let n = self
            .nodes
            .entry(format!("{group}\0{node}").into())
            .or_default();
        let e = Entry {
            name: name.into(),
            datatype,
            transient,
        };
        if n.set_alias(alias, e) {
            n.len += 1;
            self.total += 1;
        }
        true
    }
}

/// What one edge node defined (see [`Definitions::node`]).
#[derive(Debug, Clone, Copy)]
pub struct NodeDefs<'a>(&'a Node);

impl<'a> NodeDefs<'a> {
    /// The metric bound to `alias`.
    #[must_use]
    pub fn by_alias(self, alias: u64) -> Option<Def<'a>> {
        self.0.alias(alias).map(Entry::def)
    }

    /// Datatype and transient flag of the metric `name` of `device` (`""`: the node).
    #[must_use]
    pub fn by_name(self, device: &str, name: &str) -> Option<(u32, bool)> {
        self.0.named.get(device)?.get(name).copied()
    }
}

/// The value of a metric whose message left the datatype out, read as the datatype its BIRTH
/// gave: integers travel as the two's complement at the datatype's width.
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
pub fn retype(v: Value<'_>, ty: u32) -> Value<'_> {
    use datatype as dt;
    match (v, ty) {
        (Value::UInt(v), dt::INT8) => Value::Int(i64::from(v as u8 as i8)),
        (Value::UInt(v), dt::INT16) => Value::Int(i64::from(v as u16 as i16)),
        (Value::UInt(v), dt::INT32) => Value::Int(i64::from(v as u32 as i32)),
        (Value::UInt(v), dt::INT64 | dt::DATETIME) => Value::Int(v as i64),
        (Value::UInt(v), dt::UINT8) => Value::UInt(v & 0xff),
        (Value::UInt(v), dt::UINT16) => Value::UInt(v & 0xffff),
        (Value::UInt(v), dt::UINT32) => Value::UInt(v & 0xffff_ffff),
        (Value::Str(_), t) if !matches!(t, dt::STRING | dt::TEXT | dt::UUID) => Value::Unsupported,
        (v, _) => v,
    }
}

/// The payload of a rebirth request (`NCMD` with `Node Control/Rebirth` = true): asks an edge
/// node to publish its BIRTH again, so that a host that does not know its aliases learns them.
#[must_use]
pub fn rebirth_request(now_ms: u64) -> Vec<u8> {
    fn varint(mut v: u64, out: &mut Vec<u8>) {
        while v >= 0x80 {
            out.push((v as u8) | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
    }
    let name = b"Node Control/Rebirth";
    let mut m = Vec::with_capacity(32);
    m.push(0x0a); // 1: name
    varint(name.len() as u64, &mut m);
    m.extend_from_slice(name);
    m.push(0x18); // 3: timestamp
    varint(now_ms, &mut m);
    m.push(0x20); // 4: datatype
    varint(u64::from(datatype::BOOLEAN), &mut m);
    m.push(0x70); // 14: boolean_value
    m.push(1);
    let mut p = Vec::with_capacity(m.len() + 16);
    p.push(0x08); // 1: timestamp
    varint(now_ms, &mut p);
    p.push(0x12); // 2: metrics
    varint(m.len() as u64, &mut p);
    p.extend_from_slice(&m);
    p
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal protobuf encoder for tests (field, wire type, payload).
    fn varint(mut v: u64, out: &mut Vec<u8>) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                return;
            }
            out.push(b | 0x80);
        }
    }
    fn tag(field: u64, wire: u64, out: &mut Vec<u8>) {
        varint(field << 3 | wire, out);
    }
    fn len(field: u64, body: &[u8], out: &mut Vec<u8>) {
        tag(field, 2, out);
        varint(body.len() as u64, out);
        out.extend_from_slice(body);
    }

    #[test]
    fn decodes_metrics_of_every_scalar_type() {
        let mut m1 = Vec::new();
        len(1, b"Motor/Temp", &mut m1);
        tag(3, 0, &mut m1);
        varint(1_762_700_000_123, &mut m1);
        tag(4, 0, &mut m1);
        varint(u64::from(datatype::DOUBLE), &mut m1);
        tag(13, 1, &mut m1);
        m1.extend_from_slice(&41.5f64.to_le_bytes());
        let mut m2 = Vec::new();
        tag(2, 0, &mut m2);
        varint(7, &mut m2);
        tag(4, 0, &mut m2);
        varint(u64::from(datatype::INT16), &mut m2);
        tag(10, 0, &mut m2);
        varint(u64::from((-12i16) as u16), &mut m2); // two's complement at 16 bits
        let mut m3 = Vec::new();
        len(1, b"State", &mut m3);
        tag(4, 0, &mut m3);
        varint(u64::from(datatype::STRING), &mut m3);
        len(15, b"RUNNING", &mut m3);
        let mut m4 = Vec::new();
        len(1, b"Missing", &mut m4);
        tag(4, 0, &mut m4);
        varint(u64::from(datatype::FLOAT), &mut m4);
        tag(7, 0, &mut m4);
        varint(1, &mut m4);
        let mut p = Vec::new();
        tag(1, 0, &mut p);
        varint(1_762_700_000_000, &mut p);
        for m in [&m1, &m2, &m3, &m4] {
            len(2, m, &mut p);
        }
        tag(3, 0, &mut p);
        varint(42, &mut p);
        // an unknown field of each wire type is skipped
        tag(99, 0, &mut p);
        varint(5, &mut p);
        tag(98, 5, &mut p);
        p.extend_from_slice(&[0; 4]);
        let mut got = Vec::new();
        let h = for_each_metric(&p, |m| got.push(m)).unwrap();
        assert_eq!(
            h,
            Header {
                timestamp: Some(1_762_700_000_000),
                seq: Some(42),
                metrics: 4,
            }
        );
        assert_eq!(got.len(), 4);
        assert_eq!(got[0].name, Some(&b"Motor/Temp"[..]));
        assert_eq!(got[0].timestamp, Some(1_762_700_000_123));
        assert_eq!(got[0].value, Value::Float(41.5));
        assert_eq!((got[1].alias, got[1].value), (Some(7), Value::Int(-12)));
        assert_eq!(got[2].value, Value::Str(b"RUNNING"));
        assert_eq!(got[3].value, Value::Null);
    }

    #[test]
    fn refuses_truncated_payloads() {
        for bad in [&[0x12u8, 0x05, 0x0a][..], &[0x08, 0xff], &[0x00], &[0x0b]] {
            assert!(for_each_metric(bad, |_| {}).is_err(), "{bad:?}");
        }
        // eleven continuation bytes: not a varint
        assert!(
            for_each_metric(
                &[
                    0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01
                ],
                |_| {}
            )
            .is_err()
        );
    }

    #[test]
    fn topics() {
        let t = Topic::parse("spBv1.0/plant1/DDATA/gateway7/press03").unwrap();
        assert_eq!(
            (t.group, t.kind, t.node, t.device),
            ("plant1", "DDATA", "gateway7", "press03")
        );
        assert!(t.has_readings() && !t.is_birth());
        let n = Topic::parse("spBv1.0/plant1/NBIRTH/gateway7").unwrap();
        assert!(n.is_birth() && n.device.is_empty());
        for bad in [
            "spAv1.0/g/NDATA/n",
            "spBv1.0/g/NDATA",
            "spBv1.0//NDATA/n",
            "spBv1.0/g/DDATA/n/d/x",
        ] {
            assert!(Topic::parse(bad).is_none(), "{bad}");
        }
        assert!(!Topic::parse("spBv1.0/g/NDEATH/n").unwrap().has_readings());
    }
}
