//! `InfluxDB` line protocol (what Telegraf, the `InfluxDB` client libraries and most metric
//! collectors emit), read in place and without allocation.
//!
//! ```text
//! measurement[,tag=value...] field=value[,field=value...] [timestamp]
//! ```
//!
//! A message holds one reading per line. [`Lines`] cuts it into lines, [`parse`] reads one line
//! into a reusable [`Line`]: the measurement, tags and fields as **spans** of the message (their
//! byte offsets, no copy), each field value with its [`Kind`], and the timestamp as the integer
//! the line carries (its unit is the reader's business). Nothing here knows where the message
//! came from: an MQTT payload, a Kafka record, a line of a file or the body of an HTTP write take
//! the same path.
//!
//! The grammar is the one of the `InfluxDB` 3 parser (the `influxdb-line-protocol` crate, itself
//! written against the Go parser of `InfluxDB` 1 and 2): `tests/line_protocol.rs` checks this
//! module against it, line by line, on generated and on damaged input. In full:
//!
//! - **Lines.** A newline ends a line, except inside the double quotes of a string field value
//!   and directly after a backslash (the exact rule is at `line_end`). Lines that are empty or
//!   start with `#` are skipped. Spaces and tabs before a line are ignored; so are spaces and one
//!   carriage return after it.
//! - **Measurement**: up to the first unescaped comma or space. `\,`, `\ ` and `\\` stand for
//!   the character after the backslash.
//! - **Tags** (optional): `,key=value` pairs. A key ends at the first unescaped `=` (so a comma
//!   that is not escaped belongs to the key), a value at the first unescaped comma or space.
//!   `\,`, `\=`, `\ ` and `\\` stand for the character after the backslash. Tag values are
//!   always text.
//! - **Fields** (at least one), after one or more spaces: `key=value` pairs separated by commas;
//!   keys as for tags. A value is a float (`1`, `-1.5`, `1e-3`, `1.5E+3`: digits, an optional
//!   fraction, an optional exponent), an integer (`-12i`, within `i64`), an unsigned integer
//!   (`12u`, within `u64`), a boolean (`t`, `T`, `true`, `True`, `TRUE`, `f`, `F`, `false`,
//!   `False`, `FALSE`) or a string in double quotes, in which `\"` and `\\` stand for `"` and
//!   `\`, and which may hold commas, spaces and newlines.
//! - **Timestamp** (optional), after one or more spaces: an integer within `i64`.
//! - A backslash before any other character is kept, with that character. A measurement, a tag
//!   key, a tag value or a field key may not be empty nor end with a backslash; a measurement
//!   may not contain a NUL byte. A tab separates nothing.
//! - A line must be UTF-8.
//!
//! Three deliberate differences from the reference parser, each asserted by that test:
//!
//! - a carriage return before the newline is accepted (the reference refuses `\r\n` line ends);
//! - spaces after a line without a timestamp are accepted (the reference accepts them only
//!   after a timestamp);
//! - a comma after the first field with no field behind it (`m v=1,`) is refused, as the Go
//!   parser refuses it (the reference accepts it there, and only there).
//!
//! Limits: [`MAX_KEYS_PER_LINE`] tags plus fields per line; a message of at most 4 GiB (spans
//! are 32 bits). Every index into the message is checked: arbitrary bytes give an error, never
//! a panic (`fuzz/fuzz_targets/line_protocol.rs`).

use memchr::{memchr, memchr2, memchr3};

/// Most tags plus fields one line may carry: bounds the memory one line can take.
pub const MAX_KEYS_PER_LINE: usize = 4096;

/// Why a line was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineError(pub &'static str);

/// Which backslash escapes apply to a piece of text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Escapes {
    /// A measurement: `\,`, `\ `, `\\`.
    Measurement,
    /// A tag key, a tag value or a field key: `\,`, `\=`, `\ `, `\\`.
    Key,
    /// A string field value: `\"`, `\\`.
    String,
}

impl Escapes {
    fn set(self) -> &'static [u8] {
        match self {
            Self::Measurement => b", \\",
            Self::Key => b",= \\",
            Self::String => b"\"\\",
        }
    }
}

/// Decode the backslash escapes of `raw` (the bytes of a [`Text`] or of a [`Kind::StrEscaped`]
/// value) into `out`, which is cleared first. A backslash before a character that needs no
/// escape is kept.
pub fn unescape(raw: &str, escapes: Escapes, out: &mut String) {
    out.clear();
    let set = escapes.set();
    let b = raw.as_bytes();
    let (mut from, mut i) = (0, 0);
    while let Some(p) = b.get(i..).and_then(|rest| memchr(b'\\', rest)) {
        let at = i + p;
        match b.get(at + 1) {
            Some(next) if set.contains(next) => {
                // backslashes and the characters after them are ASCII: both cuts are on
                // character boundaries
                out.push_str(raw.get(from..at).unwrap_or(""));
                from = at + 1;
                i = at + 2;
            }
            _ => i = at + 1,
        }
    }
    out.push_str(raw.get(from..).unwrap_or(""));
}

/// A measurement, a tag key, a tag value or a field key: where its bytes are in the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Text {
    start: u32,
    end: u32,
    escaped: bool,
}

impl Text {
    /// The bytes as written in `msg` (the message the line was parsed from).
    #[inline]
    #[must_use]
    pub fn raw<'a>(&self, msg: &'a [u8]) -> &'a [u8] {
        msg.get(self.start as usize..self.end as usize)
            .unwrap_or(&[])
    }

    /// The text as written (a parsed line is UTF-8).
    #[inline]
    #[must_use]
    pub fn as_str<'a>(&self, msg: &'a [u8]) -> &'a str {
        std::str::from_utf8(self.raw(msg)).unwrap_or("")
    }

    /// True when the text holds a backslash: decode it with [`unescape`] before use.
    #[inline]
    #[must_use]
    pub fn is_escaped(&self) -> bool {
        self.escaped
    }

    /// First byte in the message.
    #[inline]
    #[must_use]
    pub fn start(&self) -> u32 {
        self.start
    }

    /// One past the last byte.
    #[inline]
    #[must_use]
    pub fn end(&self) -> u32 {
        self.end
    }
}

/// Type of a field value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A float: the span is the number as written (`-?digits[.digits][e[+-]digits]`).
    Float,
    /// An integer (`12i`): the span is the number without its suffix, within `i64`.
    Int,
    /// An unsigned integer (`12u`): the span is the digits without the suffix, within `u64`.
    UInt,
    /// `t`, `T`, `true`, `True` or `TRUE`.
    True,
    /// `f`, `F`, `false`, `False` or `FALSE`.
    False,
    /// A string without escapes: the span is the bytes between the quotes.
    Str,
    /// A string with `\"` or `\\`: decode with [`unescape`] ([`Escapes::String`]) before use.
    StrEscaped,
}

/// A field value: its kind and where its bytes are in the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Value {
    /// Kind.
    pub kind: Kind,
    start: u32,
    end: u32,
}

impl Value {
    /// The value's bytes in `msg` (see [`Kind`] for what they cover).
    #[inline]
    #[must_use]
    pub fn raw<'a>(&self, msg: &'a [u8]) -> &'a [u8] {
        msg.get(self.start as usize..self.end as usize)
            .unwrap_or(&[])
    }

    /// The value's text in `msg`.
    #[inline]
    #[must_use]
    pub fn as_str<'a>(&self, msg: &'a [u8]) -> &'a str {
        std::str::from_utf8(self.raw(msg)).unwrap_or("")
    }

    /// First byte in the message.
    #[inline]
    #[must_use]
    pub fn start(&self) -> u32 {
        self.start
    }

    /// One past the last byte.
    #[inline]
    #[must_use]
    pub fn end(&self) -> u32 {
        self.end
    }
}

/// One parsed line. Reused from line to line: its buffers keep their capacity.
#[derive(Debug, Clone, Default)]
pub struct Line {
    /// Measurement.
    pub measurement: Text,
    /// Tags, in the order of the line: key and value.
    pub tags: Vec<(Text, Text)>,
    /// Fields, in the order of the line: key and value.
    pub fields: Vec<(Text, Value)>,
    /// Timestamp as written (an integer; the line does not say its unit).
    pub timestamp: Option<i64>,
}

/// The extent of one line that holds data, as [`Lines`] found it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Chunk {
    start: usize,
    end: usize,
}

impl Chunk {
    /// The line's bytes in `msg`.
    #[inline]
    #[must_use]
    pub fn bytes<'a>(&self, msg: &'a [u8]) -> &'a [u8] {
        msg.get(self.start..self.end).unwrap_or(&[])
    }
}

/// The lines of a message that hold data: comments and blank lines are skipped, the white space
/// around a line is left out.
#[derive(Debug, Clone)]
pub struct Lines<'a> {
    msg: &'a [u8],
    pos: usize,
}

impl<'a> Lines<'a> {
    /// The lines of `msg`.
    #[must_use]
    pub fn new(msg: &'a [u8]) -> Self {
        Self { msg, pos: 0 }
    }
}

impl Iterator for Lines<'_> {
    type Item = Chunk;

    fn next(&mut self) -> Option<Chunk> {
        loop {
            let start = self.pos;
            // `None` once the position has passed the end (one past it after the last line)
            let rest = self.msg.get(start..)?;
            // a line without quotes or backslashes (the common case) ends at its newline: one
            // vectorised search finds it
            let end = match memchr3(b'\n', b'"', b'\\', rest) {
                None => self.msg.len(),
                Some(p) if rest.get(p) == Some(&b'\n') => start + p,
                Some(_) => line_end(self.msg, start),
            };
            self.pos = end + 1;
            if let Some(chunk) = trim(self.msg, start, end) {
                return Some(chunk);
            }
        }
    }
}

/// Where the line that starts at `start` ends (its newline, or the end of the message), for a
/// line with quotes or backslashes. The rule is the one of the `InfluxDB` parsers (`scanLine` in
/// Go, `split_lines` in Rust): a byte after a backslash is skipped; after the first space of the
/// line (the field set), `=` and `,` outside quotes are counted and a double quote opens or
/// closes a string when more `=` than `,` have been seen (it follows a field key); a newline
/// outside quotes ends the line.
fn line_end(msg: &[u8], start: usize) -> usize {
    let (mut quoted, mut fields) = (false, false);
    let (mut equals, mut commas) = (0u32, 0u32);
    let mut i = start;
    while let Some(&c) = msg.get(i) {
        match c {
            b'\\' => i += 1,
            b' ' => fields = true,
            b'=' if fields && !quoted => equals += 1,
            b',' if fields && !quoted => commas += 1,
            b'"' if fields && equals > commas => quoted = !quoted,
            b'\n' if !quoted => return i,
            _ => {}
        }
        i += 1;
    }
    msg.len()
}

/// The part of `msg[start..end]` that holds a line: without one trailing carriage return and
/// the spaces before it, without leading spaces, tabs, newlines and comments. `None` when
/// nothing is left.
fn trim(msg: &[u8], mut start: usize, mut end: usize) -> Option<Chunk> {
    if end > start && msg.get(end - 1) == Some(&b'\r') {
        end -= 1;
    }
    while end > start && msg.get(end - 1) == Some(&b' ') {
        end -= 1;
    }
    loop {
        while start < end && matches!(msg.get(start), Some(b' ' | b'\t' | b'\n')) {
            start += 1;
        }
        if start < end && msg.get(start) == Some(&b'#') {
            // a comment runs to the next newline (a line holds one only inside quotes or
            // after a backslash)
            start = msg
                .get(start..end)
                .and_then(|c| memchr(b'\n', c))
                .map_or(end, |p| start + p);
        } else {
            break;
        }
    }
    (start < end).then_some(Chunk { start, end })
}

// Byte classes of the grammar.
const WS: u8 = 1; // space, tab, newline
const NUL: u8 = 2;
const COMMA: u8 = 4;
const EQ: u8 = 8;
const BSL: u8 = 16;

static CLASS: [u8; 256] = {
    let mut t = [0u8; 256];
    t[b' ' as usize] = WS;
    t[b'\t' as usize] = WS;
    t[b'\n' as usize] = WS;
    t[0] = NUL;
    t[b',' as usize] = COMMA;
    t[b'=' as usize] = EQ;
    t[b'\\' as usize] = BSL;
    t
};

const END_MEASUREMENT: u8 = WS | NUL | COMMA;
const END_KEY: u8 = WS | EQ;
const END_TAG_VALUE: u8 = WS | COMMA;

/// A name starting at `i`: it runs to the first byte of a class in `stop` that no backslash
/// precedes (or to the end of `b`). Returns the name and the position after it.
#[inline]
fn name(b: &[u8], start: usize, stop: u8, what: &'static str) -> Result<(Text, usize), LineError> {
    let mask = stop | BSL;
    let mut i = start;
    let mut escaped = false;
    // position after the last `\\` pair: a name may not end there
    let mut after_backslash = usize::MAX;
    loop {
        while let Some(&c) = b.get(i) {
            if CLASS[usize::from(c)] & mask != 0 {
                break;
            }
            i += 1;
        }
        if b.get(i) != Some(&b'\\') {
            break;
        }
        // a backslash takes the next byte with it, whatever it is
        let Some(&next) = b.get(i + 1) else {
            return Err(LineError("a name ends with a backslash"));
        };
        escaped = true;
        i += 2;
        if next == b'\\' {
            after_backslash = i;
        }
    }
    if i == start {
        return Err(LineError(what));
    }
    if after_backslash == i {
        return Err(LineError("a name ends with a backslash"));
    }
    Ok((span(start, i, escaped), i))
}

#[inline]
fn span(start: usize, end: usize, escaped: bool) -> Text {
    // `parse` refuses lines that end beyond 4 GiB
    Text {
        start: start as u32,
        end: end as u32,
        escaped,
    }
}

/// The measurement of a line, without reading the rest of it (to tell lines apart before
/// parsing them).
pub fn measurement(msg: &[u8], chunk: Chunk) -> Result<Text, LineError> {
    let b = bounded(msg, chunk)?;
    name(b, chunk.start, END_MEASUREMENT, "no measurement").map(|(m, _)| m)
}

/// The message up to the end of the line.
fn bounded(msg: &[u8], chunk: Chunk) -> Result<&[u8], LineError> {
    if u32::try_from(chunk.end).is_err() {
        return Err(LineError("message longer than 4 GiB"));
    }
    msg.get(..chunk.end)
        .filter(|_| chunk.start <= chunk.end)
        .ok_or(LineError("line outside the message"))
}

/// Parse the line at `chunk` of `msg` into `line` (cleared first; after an error its content is
/// unspecified).
pub fn parse(msg: &[u8], chunk: Chunk, line: &mut Line) -> Result<(), LineError> {
    line.tags.clear();
    line.fields.clear();
    line.timestamp = None;
    line.measurement = Text::default();
    let b = bounded(msg, chunk)?;
    if std::str::from_utf8(b.get(chunk.start..).unwrap_or(&[])).is_err() {
        return Err(LineError("line is not utf-8"));
    }
    let (m, mut i) = name(b, chunk.start, END_MEASUREMENT, "no measurement")?;
    line.measurement = m;
    if b.get(i) == Some(&b',') {
        loop {
            let (key, eq) = name(b, i + 1, END_KEY, "tag without a key")?;
            if b.get(eq) != Some(&b'=') {
                return Err(LineError("tag without a value"));
            }
            let (value, next) = name(b, eq + 1, END_TAG_VALUE, "tag without a value")?;
            if line.tags.len() >= MAX_KEYS_PER_LINE {
                return Err(LineError("too many tags and fields in one line"));
            }
            line.tags.push((key, value));
            i = next;
            if b.get(i) != Some(&b',') {
                break;
            }
        }
    }
    if b.get(i) != Some(&b' ') {
        return Err(LineError("no fields"));
    }
    i = skip_spaces(b, i);
    loop {
        let (key, eq) = name(b, i, END_KEY, "no fields")?;
        if b.get(eq) != Some(&b'=') {
            return Err(LineError("field without a value"));
        }
        let (value, next) = value(b, eq + 1)?;
        if line.tags.len() + line.fields.len() >= MAX_KEYS_PER_LINE {
            return Err(LineError("too many tags and fields in one line"));
        }
        line.fields.push((key, value));
        i = next;
        if b.get(i) != Some(&b',') {
            break;
        }
        i += 1;
    }
    // a value ends at a comma, a space or the end of the line
    i = skip_spaces(b, i);
    if i < b.len() {
        line.timestamp = Some(timestamp(b, i)?);
    }
    Ok(())
}

#[inline]
fn skip_spaces(b: &[u8], mut i: usize) -> usize {
    while b.get(i) == Some(&b' ') {
        i += 1;
    }
    i
}

#[inline]
fn digits(b: &[u8], mut i: usize) -> usize {
    while b.get(i).is_some_and(u8::is_ascii_digit) {
        i += 1;
    }
    i
}

/// The timestamp that fills the rest of the line from `i`.
fn timestamp(b: &[u8], i: usize) -> Result<i64, LineError> {
    let d0 = i + usize::from(b.get(i) == Some(&b'-'));
    let end = digits(b, d0);
    if end == d0 || end != b.len() {
        return Err(LineError("malformed timestamp"));
    }
    integer(b, i, end).ok_or(LineError("timestamp out of range"))
}

/// The digits (and sign) at `b[start..end]` as an integer; `None` when out of range.
#[inline]
fn integer<T: std::str::FromStr>(b: &[u8], start: usize, end: usize) -> Option<T> {
    std::str::from_utf8(b.get(start..end)?).ok()?.parse().ok()
}

/// The field value at `i`. Returns it and the position after it, which is a comma, a space or
/// the end of the line.
#[inline]
fn value(b: &[u8], i: usize) -> Result<(Value, usize), LineError> {
    const BAD: LineError = LineError("malformed field value");
    let first = *b.get(i).ok_or(LineError("field without a value"))?;
    if first == b'"' {
        return string(b, i);
    }
    let neg = first == b'-';
    let d0 = i + usize::from(neg);
    let mut j = digits(b, d0);
    let (kind, end, next) = if j == d0 {
        if neg {
            return Err(BAD);
        }
        // not a number: a boolean, alone up to the next comma or space
        let end = b
            .get(i..)
            .and_then(|rest| memchr2(b',', b' ', rest))
            .map_or(b.len(), |p| i + p);
        let kind = match b.get(i..end).unwrap_or(&[]) {
            b"t" | b"T" | b"true" | b"True" | b"TRUE" => Kind::True,
            b"f" | b"F" | b"false" | b"False" | b"FALSE" => Kind::False,
            _ => return Err(BAD),
        };
        (kind, end, end)
    } else {
        match b.get(j) {
            Some(b'i') => {
                // 18 digits always fit
                if j - d0 > 18 && integer::<i64>(b, i, j).is_none() {
                    return Err(LineError("integer out of range"));
                }
                (Kind::Int, j, j + 1)
            }
            Some(b'u') if !neg => {
                if j - d0 > 19 && integer::<u64>(b, i, j).is_none() {
                    return Err(LineError("integer out of range"));
                }
                (Kind::UInt, j, j + 1)
            }
            _ => {
                if b.get(j) == Some(&b'.') {
                    let f0 = j + 1;
                    j = digits(b, f0);
                    if j == f0 {
                        return Err(BAD);
                    }
                }
                if matches!(b.get(j), Some(b'e' | b'E')) {
                    let e0 = j + 1 + usize::from(matches!(b.get(j + 1), Some(b'+' | b'-')));
                    j = digits(b, e0);
                    if j == e0 {
                        return Err(BAD);
                    }
                }
                (Kind::Float, j, j)
            }
        }
    };
    if !matches!(b.get(next), None | Some(b',' | b' ')) {
        return Err(BAD);
    }
    let v = Value {
        kind,
        start: i as u32,
        end: end as u32,
    };
    Ok((v, next))
}

/// The string value whose opening quote is at `i`.
fn string(b: &[u8], i: usize) -> Result<(Value, usize), LineError> {
    const OPEN: LineError = LineError("string value without its closing quote");
    let start = i + 1;
    let mut j = start;
    let mut escaped = false;
    loop {
        j += b
            .get(j..)
            .and_then(|rest| memchr2(b'"', b'\\', rest))
            .ok_or(OPEN)?;
        if b.get(j) == Some(&b'"') {
            break;
        }
        // `\"` and `\\` are escapes; any other backslash is itself
        if matches!(b.get(j + 1), Some(b'"' | b'\\')) {
            escaped = true;
            j += 2;
        } else {
            j += 1;
        }
    }
    if !matches!(b.get(j + 1), None | Some(b',' | b' ')) {
        return Err(LineError("malformed field value"));
    }
    let v = Value {
        kind: if escaped { Kind::StrEscaped } else { Kind::Str },
        start: start as u32,
        end: j as u32,
    };
    Ok((v, j + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse a message of one line.
    fn one(text: &str) -> Result<Line, LineError> {
        let msg = text.as_bytes();
        let mut lines = Lines::new(msg);
        let chunk = lines.next().expect("a line");
        assert!(lines.next().is_none(), "one line expected in {text:?}");
        let mut line = Line::default();
        parse(msg, chunk, &mut line).map(|()| line)
    }

    fn text(t: Text, msg: &str, escapes: Escapes) -> String {
        let mut out = String::new();
        unescape(t.as_str(msg.as_bytes()), escapes, &mut out);
        out
    }

    fn field<'a>(l: &Line, msg: &'a str, i: usize) -> (String, Kind, &'a str) {
        let (k, v) = l.fields[i];
        (text(k, msg, Escapes::Key), v.kind, v.as_str(msg.as_bytes()))
    }

    #[test]
    fn a_telegraf_line() {
        let msg =
            "cpu,cpu=cpu0,host=web-01 usage_idle=98.2,usage_user=1.1,n=3i 1762700000000000000";
        let l = one(msg).unwrap();
        assert_eq!(l.measurement.as_str(msg.as_bytes()), "cpu");
        assert!(!l.measurement.is_escaped());
        let tags: Vec<(&str, &str)> = l
            .tags
            .iter()
            .map(|(k, v)| (k.as_str(msg.as_bytes()), v.as_str(msg.as_bytes())))
            .collect();
        assert_eq!(tags, [("cpu", "cpu0"), ("host", "web-01")]);
        assert_eq!(
            field(&l, msg, 0),
            ("usage_idle".into(), Kind::Float, "98.2")
        );
        assert_eq!(field(&l, msg, 1), ("usage_user".into(), Kind::Float, "1.1"));
        assert_eq!(field(&l, msg, 2), ("n".into(), Kind::Int, "3"));
        assert_eq!(l.timestamp, Some(1_762_700_000_000_000_000));
        // no tags, no timestamp
        let l = one("m v=1").unwrap();
        assert_eq!(l.tags.len(), 0);
        assert_eq!(l.timestamp, None);
    }

    #[test]
    fn field_value_types() {
        let msg =
            r#"m a=1,b=-1.5,c=1e-3,d=1.5E+3,e=-12i,f=12u,g=t,h=FALSE,i="x y",j="",k=007,l=-0 5"#;
        let l = one(msg).unwrap();
        let got: Vec<(Kind, &str)> = (0..l.fields.len())
            .map(|i| {
                let (_, k, v) = field(&l, msg, i);
                (k, v)
            })
            .collect();
        assert_eq!(
            got,
            [
                (Kind::Float, "1"),
                (Kind::Float, "-1.5"),
                (Kind::Float, "1e-3"),
                (Kind::Float, "1.5E+3"),
                (Kind::Int, "-12"),
                (Kind::UInt, "12"),
                (Kind::True, "t"),
                (Kind::False, "FALSE"),
                (Kind::Str, "x y"),
                (Kind::Str, ""),
                (Kind::Float, "007"),
                (Kind::Float, "-0"),
            ]
        );
        for b in ["t", "T", "true", "True", "TRUE"] {
            assert_eq!(
                one(&format!("m v={b}")).unwrap().fields[0].1.kind,
                Kind::True
            );
        }
        for b in ["f", "F", "false", "False", "FALSE"] {
            assert_eq!(
                one(&format!("m v={b}")).unwrap().fields[0].1.kind,
                Kind::False
            );
        }
        // integers at the edges of their ranges
        for ok in [
            "9223372036854775807i",
            "-9223372036854775808i",
            "18446744073709551615u",
            "0u",
        ] {
            assert!(one(&format!("m v={ok}")).is_ok(), "{ok}");
        }
        for bad in [
            "9223372036854775808i",
            "-9223372036854775809i",
            "18446744073709551616u",
            "99999999999999999999999i",
        ] {
            assert_eq!(
                one(&format!("m v={bad}")).unwrap_err(),
                LineError("integer out of range"),
                "{bad}"
            );
        }
    }

    #[test]
    fn malformed_values_are_refused() {
        for bad in [
            "",
            "-",
            "+1",
            ".5",
            "-.5",
            "1.",
            "1.e5",
            "1e",
            "1e+",
            "1.5e",
            "1.5.5",
            "1x",
            "-1u",
            "1ii",
            "1i5",
            "i",
            "u",
            "tr",
            "tRUE",
            "truex",
            "yes",
            "nan",
            "inf",
            "0x10",
            "1_000",
            "\"open",
            "\"a\"b",
            "\"a\"\"b\"",
            "'a'",
            "1\t",
            "\0",
        ] {
            assert!(one(&format!("m v={bad}")).is_err(), "{bad:?}");
            assert!(
                one(&format!("m v={bad} 5")).is_err(),
                "{bad:?} with a timestamp"
            );
            assert!(
                one(&format!("m v={bad},w=1")).is_err(),
                "{bad:?} before a field"
            );
        }
    }

    #[test]
    fn escapes_in_names() {
        let msg = r"we\,a\ ther\\x,ta\=g\,k\ 1=va\,l\=u\ e fi\=e\,l\ d=1";
        let l = one(msg).unwrap();
        assert!(l.measurement.is_escaped());
        assert_eq!(
            text(l.measurement, msg, Escapes::Measurement),
            r"we,a ther\x"
        );
        assert_eq!(text(l.tags[0].0, msg, Escapes::Key), "ta=g,k 1");
        assert_eq!(text(l.tags[0].1, msg, Escapes::Key), "va,l=u e");
        assert_eq!(field(&l, msg, 0).0, "fi=e,l d");
        // a backslash before any other character stays, with the character; `=` needs no
        // escape in a measurement (and keeps its backslash there) nor in a tag value
        let msg = r"m\=a\b,k\x=a=b\n f\t=1";
        let l = one(msg).unwrap();
        assert_eq!(text(l.measurement, msg, Escapes::Measurement), r"m\=a\b");
        assert_eq!(text(l.tags[0].0, msg, Escapes::Key), r"k\x");
        assert_eq!(text(l.tags[0].1, msg, Escapes::Key), r"a=b\n");
        assert_eq!(field(&l, msg, 0).0, r"f\t");
        // an unescaped comma belongs to a tag key or a field key (as in the reference parser)
        let msg = "m,a,b=1 c,d=2";
        let l = one(msg).unwrap();
        assert_eq!(l.tags[0].0.as_str(msg.as_bytes()), "a,b");
        assert_eq!(field(&l, msg, 0).0, "c,d");
        // an escaped newline does not end the line
        let msg = "m,k=a\\\nb v=1";
        let l = one(msg).unwrap();
        assert_eq!(text(l.tags[0].1, msg, Escapes::Key), "a\\\nb");
    }

    #[test]
    fn names_may_not_be_empty_nor_end_with_a_backslash() {
        for bad in [
            ",k=v f=1",
            "m, f=1",
            "m,k f=1",
            "m,k= f=1",
            "m,=v f=1",
            "m,k=v, f=1",
            "m,k=v,x f=1",
            "m =1",
            "m f",
            "m f=",
            "m f=1,",
            "m f=1,g",
            "m f=1,=2",
            "m",
            "m\tf=1",
            "m\0x f=1",
            "m\\\\ f=1",
            "m,k\\\\=v f=1",
            "m,k=v\\\\ f=1",
            "m f\\\\=1",
            "m\\",
            "m,k=v\\",
        ] {
            assert!(one(bad).is_err(), "{bad:?}");
        }
        // NUL is refused in a measurement only
        assert!(one("m,k\0=v\0 f\0=\"\0\"").is_ok());
    }

    #[test]
    fn strings() {
        let msg = r#"m a="say \"hi\"",b="back\\slash",c="lone \ and \n stay",d="a,b c=d""#;
        let l = one(msg).unwrap();
        let s = |i: usize| {
            let (_, v) = l.fields[i];
            let mut out = String::new();
            unescape(v.as_str(msg.as_bytes()), Escapes::String, &mut out);
            (v.kind, out)
        };
        assert_eq!(s(0), (Kind::StrEscaped, r#"say "hi""#.to_owned()));
        assert_eq!(s(1), (Kind::StrEscaped, r"back\slash".to_owned()));
        assert_eq!(s(2), (Kind::Str, r"lone \ and \n stay".to_owned()));
        assert_eq!(s(3), (Kind::Str, "a,b c=d".to_owned()));
        // a string may hold a newline: the line goes on
        let msg = "m a=\"two\nlines\",b=1 7\nm a=\"x\" 8";
        let chunks: Vec<&str> = Lines::new(msg.as_bytes())
            .map(|c| std::str::from_utf8(c.bytes(msg.as_bytes())).unwrap())
            .collect();
        assert_eq!(chunks, ["m a=\"two\nlines\",b=1 7", "m a=\"x\" 8"]);
        let mut line = Line::default();
        let first = Lines::new(msg.as_bytes()).next().unwrap();
        parse(msg.as_bytes(), first, &mut line).unwrap();
        assert_eq!(line.fields[0].1.as_str(msg.as_bytes()), "two\nlines");
        assert_eq!(line.timestamp, Some(7));
    }

    #[test]
    fn timestamps() {
        assert_eq!(one("m v=1 0").unwrap().timestamp, Some(0));
        assert_eq!(one("m v=1 -5").unwrap().timestamp, Some(-5));
        assert_eq!(one("m v=1   42").unwrap().timestamp, Some(42));
        assert_eq!(
            one("m v=1 9223372036854775807").unwrap().timestamp,
            Some(i64::MAX)
        );
        assert_eq!(
            one("m v=1 -9223372036854775808").unwrap().timestamp,
            Some(i64::MIN)
        );
        assert_eq!(
            one("m v=1 9223372036854775808").unwrap_err(),
            LineError("timestamp out of range")
        );
        for bad in [
            "m v=1 1.5",
            "m v=1 12x",
            "m v=1 +3",
            "m v=1 1 2",
            "m v=1 -",
            "m v=1\t5",
        ] {
            assert!(one(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn lines_comments_and_white_space() {
        let msg = "# a comment\n\n  \t m v=1 1\r\nm v=2 2  \n   # indented comment\nm v=3\r\n\n";
        let chunks: Vec<&str> = Lines::new(msg.as_bytes())
            .map(|c| std::str::from_utf8(c.bytes(msg.as_bytes())).unwrap())
            .collect();
        assert_eq!(chunks, ["m v=1 1", "m v=2 2", "m v=3"]);
        assert_eq!(Lines::new(b"").count(), 0);
        assert_eq!(Lines::new(b"\n\n# only a comment").count(), 0);
        assert_eq!(Lines::new(b"\r\n \r\n").count(), 0);
        // spaces after a line are ignored, with a timestamp or without one
        assert_eq!(one("m v=1   ").unwrap().timestamp, None);
        assert_eq!(one("m v=1 7  ").unwrap().timestamp, Some(7));
        // a carriage return anywhere else is not white space
        assert!(one("m v=1\r 7").is_err());
        // a tab does not separate the parts of a line
        assert!(one("m\tv=1").is_err());
    }

    #[test]
    fn a_line_must_be_utf8_and_bounded() {
        let msg = b"m,k=\xff v=1\nm v=2";
        let mut line = Line::default();
        let chunks: Vec<Chunk> = Lines::new(msg).collect();
        assert_eq!(chunks.len(), 2);
        assert_eq!(
            parse(msg, chunks[0], &mut line),
            Err(LineError("line is not utf-8"))
        );
        // the next line is not affected
        parse(msg, chunks[1], &mut line).unwrap();
        assert_eq!(line.fields[0].1.as_str(msg), "2");
        // multi-byte text is kept
        let msg = "température,lieu=Zürich,k\\=é=ü v=\"größe\" 1";
        let l = one(msg).unwrap();
        assert_eq!(l.measurement.as_str(msg.as_bytes()), "température");
        assert_eq!(text(l.tags[1].0, msg, Escapes::Key), "k=é");
        // tags plus fields are bounded
        let many: Vec<String> = (0..=MAX_KEYS_PER_LINE).map(|i| format!("f{i}=1")).collect();
        let msg = format!("m {}", many.join(","));
        assert_eq!(
            one(&msg).unwrap_err(),
            LineError("too many tags and fields in one line")
        );
        let msg = format!("m {}", many[1..].join(","));
        assert_eq!(one(&msg).unwrap().fields.len(), MAX_KEYS_PER_LINE);
    }

    #[test]
    fn measurement_alone() {
        let msg = b"cpu\\ 0,host=a v=1\n,broken v=1\nmem v=";
        let got: Vec<Result<String, LineError>> = Lines::new(msg)
            .map(|c| {
                measurement(msg, c).map(|m| {
                    let mut out = String::new();
                    unescape(m.as_str(msg), Escapes::Measurement, &mut out);
                    out
                })
            })
            .collect();
        assert_eq!(
            got,
            [
                Ok("cpu 0".to_owned()),
                Err(LineError("no measurement")),
                Ok("mem".to_owned())
            ]
        );
    }
}
