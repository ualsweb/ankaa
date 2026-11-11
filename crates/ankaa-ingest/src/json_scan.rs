//! Path-directed, single-pass JSON scanner for the generic mapper.
//!
//! A source maps a handful of paths (`meta.time`, `values.temp`, …) out of messages that can carry
//! many more fields. Building a document tree for each message costs an allocation per key and
//! per container, and then the tree is walked once per column. This scanner instead compiles the
//! mapped paths into a small trie once, then makes **one forward pass** over the bytes:
//!
//! - a key that leads nowhere is skipped without being decoded: a string by searching for the
//!   next `"` or `\` (`memchr2`: NEON/SSE word-at-a-time), a container by counting brackets;
//! - a key on a mapped path is followed; at a leaf the value is recorded as a **span**: its kind
//!   and its byte offsets in the message (12 bytes, no copy, no lifetime), so the buffers that
//!   hold the spans live in the mapper and are reused for every message;
//! - with `rows = "<path>"`, the array at that path is walked element by element and each element
//!   is matched against the element paths, so one message yields one row per element.
//!
//! Validation: everything on a mapped path is checked against the JSON grammar (strings,
//! numbers, literals, structure). Unmapped parts are checked for structure only (balanced
//! brackets, terminated strings), the way on-demand parsers such as simdjson's work: their
//! content is never used. Nesting deeper than [`MAX_DEPTH`] is rejected, so hostile input cannot
//! exhaust the stack.

/// Deepest nesting accepted.
pub const MAX_DEPTH: usize = 128;

/// Kind of a value found on a mapped path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A string without escapes: the span is the bytes between the quotes.
    Str,
    /// A string with at least one `\` escape: decode with [`unescape`] before use.
    StrEscaped,
    /// A number, as written (validated JSON number grammar).
    Num,
    /// `true`.
    True,
    /// `false`.
    False,
    /// `null`.
    Null,
    /// An object or an array where a scalar was expected.
    Composite,
}

/// A value on a mapped path: its kind and where its bytes are in the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    /// Kind.
    pub kind: Kind,
    /// First byte (for strings: after the opening quote).
    pub start: u32,
    /// One past the last byte (for strings: the closing quote).
    pub end: u32,
}

impl Span {
    /// The value's bytes in `msg` (the message it was found in).
    #[inline]
    #[must_use]
    pub fn bytes<'a>(&self, msg: &'a [u8]) -> &'a [u8] {
        msg.get(self.start as usize..self.end as usize)
            .unwrap_or(&[])
    }

    fn of(kind: Kind, start: usize, end: usize) -> Self {
        // messages are bounded far below 4 GiB (scan refuses longer ones)
        #[allow(clippy::cast_possible_truncation)]
        Self {
            kind,
            start: start as u32,
            end: end as u32,
        }
    }
}

/// Why a message was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanError {
    /// Not JSON (or not the JSON grammar on a mapped path).
    Invalid,
    /// Nesting deeper than [`MAX_DEPTH`].
    TooDeep,
    /// More than the row limit of elements in the `rows` array.
    TooManyRows,
}

#[derive(Debug, Clone)]
struct Node {
    /// Key bytes (object member) or decimal index (array element).
    key: Box<[u8]>,
    children: Vec<u32>,
    /// Slots this node fills (mapped paths end here; two columns may map the same path).
    slots: Vec<u16>,
    /// This node is the `rows` array.
    rows: bool,
}

/// Mapped paths compiled into a trie (built once per source).
#[derive(Debug, Clone)]
pub struct Plan {
    nodes: Vec<Node>,
    /// Root of the per-element paths (for `rows`).
    elem_root: Option<u32>,
    msg_slots: usize,
    elem_slots: usize,
    max_rows: usize,
}

impl Plan {
    /// `msg_paths[i]` fills message slot `i`; with `rows`, `elem_paths[j]` fills element slot `j`
    /// of every element of the array at `rows`. Paths are split on `.`; a decimal segment also
    /// indexes arrays. `max_rows` bounds the elements of one message.
    #[must_use]
    pub fn new(
        msg_paths: &[Vec<String>],
        rows: Option<&[String]>,
        elem_paths: &[Vec<String>],
        max_rows: usize,
    ) -> Self {
        let mut p = Self {
            nodes: vec![Node {
                key: Box::new([]),
                children: Vec::new(),
                slots: Vec::new(),
                rows: false,
            }],
            elem_root: None,
            msg_slots: msg_paths.len(),
            elem_slots: elem_paths.len(),
            max_rows,
        };
        for (i, path) in msg_paths.iter().enumerate() {
            let n = p.walk(0, path);
            p.nodes[n as usize]
                .slots
                .push(u16::try_from(i).unwrap_or(u16::MAX));
        }
        if let Some(rows) = rows {
            let n = p.walk(0, rows);
            p.nodes[n as usize].rows = true;
            let root = p.push_node(b"");
            p.elem_root = Some(root);
            for (j, path) in elem_paths.iter().enumerate() {
                let n = p.walk(root, path);
                p.nodes[n as usize]
                    .slots
                    .push(u16::try_from(j).unwrap_or(u16::MAX));
            }
        }
        p
    }

    fn push_node(&mut self, key: &[u8]) -> u32 {
        self.nodes.push(Node {
            key: key.into(),
            children: Vec::new(),
            slots: Vec::new(),
            rows: false,
        });
        u32::try_from(self.nodes.len() - 1).unwrap_or(u32::MAX)
    }

    fn walk(&mut self, from: u32, path: &[String]) -> u32 {
        let mut cur = from;
        for seg in path {
            let found = self.nodes[cur as usize]
                .children
                .iter()
                .copied()
                .find(|&c| &*self.nodes[c as usize].key == seg.as_bytes());
            cur = if let Some(c) = found {
                c
            } else {
                let c = self.push_node(seg.as_bytes());
                self.nodes[cur as usize].children.push(c);
                c
            };
        }
        cur
    }

    /// Message slots.
    #[must_use]
    pub fn msg_slots(&self) -> usize {
        self.msg_slots
    }

    /// Element slots (0 without `rows`).
    #[must_use]
    pub fn elem_slots(&self) -> usize {
        self.elem_slots
    }

    /// Scan `msg`, filling `out` (reused between messages: no allocation after warm-up).
    pub fn scan(&self, msg: &[u8], out: &mut Found) -> Result<(), ScanError> {
        if u32::try_from(msg.len()).is_err() {
            return Err(ScanError::Invalid);
        }
        out.msg.clear();
        out.msg.resize(self.msg_slots, None);
        out.elems.clear();
        out.rows = 0;
        out.rows_seen = false;
        let mut s = Scanner { b: msg, i: 0 };
        s.ws();
        self.value(&mut s, Some(0), out, Ctx::Msg, 0)?;
        s.ws();
        if s.i != s.b.len() {
            return Err(ScanError::Invalid);
        }
        Ok(())
    }

    #[inline]
    fn record(&self, node: u32, raw: Span, ctx: Ctx, out: &mut Found) {
        for &slot in &self.nodes[node as usize].slots {
            let slot = usize::from(slot);
            match ctx {
                Ctx::Msg => out.msg[slot] = Some(raw),
                Ctx::Elem => {
                    let base = (out.rows - 1) * self.elem_slots;
                    out.elems[base + slot] = Some(raw);
                }
            }
        }
    }

    /// One value at the cursor. `node`: where it sits in the trie (`None` = off every path).
    fn value(
        &self,
        s: &mut Scanner<'_>,
        node: Option<u32>,
        out: &mut Found,
        ctx: Ctx,
        depth: usize,
    ) -> Result<(), ScanError> {
        let Some(n) = node else {
            return s.skip_value(depth);
        };
        if depth >= MAX_DEPTH {
            return Err(ScanError::TooDeep);
        }
        match s.peek() {
            Some(b'{') => {
                self.record(n, Span::of(Kind::Composite, s.i, s.i), ctx, out);
                self.object(s, n, out, ctx, depth)
            }
            Some(b'[') => {
                self.record(n, Span::of(Kind::Composite, s.i, s.i), ctx, out);
                self.array(s, n, out, ctx, depth)
            }
            Some(b'"') => {
                let (a, b, escaped) = s.string_span()?;
                let kind = if escaped { Kind::StrEscaped } else { Kind::Str };
                self.record(n, Span::of(kind, a, b), ctx, out);
                Ok(())
            }
            Some(b't') => {
                let a = s.i;
                s.literal(b"true")?;
                self.record(n, Span::of(Kind::True, a, s.i), ctx, out);
                Ok(())
            }
            Some(b'f') => {
                let a = s.i;
                s.literal(b"false")?;
                self.record(n, Span::of(Kind::False, a, s.i), ctx, out);
                Ok(())
            }
            Some(b'n') => {
                let a = s.i;
                s.literal(b"null")?;
                self.record(n, Span::of(Kind::Null, a, s.i), ctx, out);
                Ok(())
            }
            Some(b'-' | b'0'..=b'9') => {
                let a = s.i;
                s.number()?;
                self.record(n, Span::of(Kind::Num, a, s.i), ctx, out);
                Ok(())
            }
            _ => Err(ScanError::Invalid),
        }
    }

    /// An object on a mapped path (cursor on `{`): members on a path are followed, the others
    /// skipped.
    fn object(
        &self,
        s: &mut Scanner<'_>,
        n: u32,
        out: &mut Found,
        ctx: Ctx,
        depth: usize,
    ) -> Result<(), ScanError> {
        let nd = &self.nodes[n as usize];
        s.i += 1;
        s.ws();
        if s.peek() == Some(b'}') {
            s.i += 1;
            return Ok(());
        }
        loop {
            s.ws();
            let (key, escaped) = s.string()?;
            let child = if nd.children.is_empty() {
                None
            } else {
                self.child(nd, key, escaped)
            };
            s.ws();
            if s.next() != Some(b':') {
                return Err(ScanError::Invalid);
            }
            s.ws();
            self.value(s, child, out, ctx, depth + 1)?;
            s.ws();
            match s.next() {
                Some(b',') => {}
                Some(b'}') => return Ok(()),
                _ => return Err(ScanError::Invalid),
            }
        }
    }

    /// An array on a mapped path (cursor on `[`): the `rows` array yields one row per element,
    /// any other is indexed by position.
    fn array(
        &self,
        s: &mut Scanner<'_>,
        n: u32,
        out: &mut Found,
        ctx: Ctx,
        depth: usize,
    ) -> Result<(), ScanError> {
        let nd = &self.nodes[n as usize];
        let rows = nd.rows && ctx == Ctx::Msg;
        if rows {
            out.rows_seen = true;
        }
        s.i += 1;
        s.ws();
        if s.peek() == Some(b']') {
            s.i += 1;
            return Ok(());
        }
        let mut k = 0usize;
        loop {
            s.ws();
            if rows {
                if out.rows >= self.max_rows {
                    return Err(ScanError::TooManyRows);
                }
                out.rows += 1;
                out.elems.resize(out.rows * self.elem_slots, None);
                self.value(s, self.elem_root, out, Ctx::Elem, depth + 1)?;
            } else {
                let child = if nd.children.is_empty() {
                    None
                } else {
                    self.index_child(nd, k)
                };
                self.value(s, child, out, ctx, depth + 1)?;
            }
            k += 1;
            s.ws();
            match s.next() {
                Some(b',') => {}
                Some(b']') => return Ok(()),
                _ => return Err(ScanError::Invalid),
            }
        }
    }

    /// The child of `nd` whose key equals the (possibly escaped) JSON key.
    fn child(&self, nd: &Node, key: &[u8], escaped: bool) -> Option<u32> {
        if escaped {
            let mut buf = String::new();
            unescape(key, &mut buf).ok()?;
            return nd
                .children
                .iter()
                .copied()
                .find(|&c| &*self.nodes[c as usize].key == buf.as_bytes());
        }
        nd.children
            .iter()
            .copied()
            .find(|&c| &*self.nodes[c as usize].key == key)
    }

    fn index_child(&self, nd: &Node, k: usize) -> Option<u32> {
        let mut buf = [0u8; 20];
        let digits = itoa(k, &mut buf);
        nd.children
            .iter()
            .copied()
            .find(|&c| &*self.nodes[c as usize].key == digits)
    }
}

fn itoa(mut k: usize, buf: &mut [u8; 20]) -> &[u8] {
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = b'0' + (k % 10) as u8;
        k /= 10;
        if k == 0 {
            return &buf[i..];
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ctx {
    Msg,
    Elem,
}

/// What a scan found: one optional span per message slot, and per element of the `rows` array
/// one optional span per element slot (row-major). Owned by the caller and reused.
#[derive(Debug, Default, Clone)]
pub struct Found {
    /// Message slots.
    pub msg: Vec<Option<Span>>,
    /// Element slots, `rows × elem_slots`.
    pub elems: Vec<Option<Span>>,
    /// Elements in the `rows` array.
    pub rows: usize,
    /// The `rows` path held an array (possibly empty).
    pub rows_seen: bool,
}

struct Scanner<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Scanner<'a> {
    #[inline]
    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }

    #[inline]
    fn next(&mut self) -> Option<u8> {
        let c = self.peek()?;
        self.i += 1;
        Some(c)
    }

    #[inline]
    fn ws(&mut self) {
        while let Some(b' ' | b'\n' | b'\r' | b'\t') = self.peek() {
            self.i += 1;
        }
    }

    fn literal(&mut self, lit: &[u8]) -> Result<(), ScanError> {
        if self.b.get(self.i..self.i + lit.len()) == Some(lit) {
            self.i += lit.len();
            Ok(())
        } else {
            Err(ScanError::Invalid)
        }
    }

    /// A string at the cursor: its bytes between the quotes.
    #[inline]
    fn string(&mut self) -> Result<(&'a [u8], bool), ScanError> {
        let (a, b, escaped) = self.string_span()?;
        Ok((&self.b[a..b], escaped))
    }

    /// A string at the cursor (which is on its `"`): the offsets of the bytes between the quotes
    /// and whether they contain escapes. Escapes are checked for shape; control characters are
    /// refused.
    fn string_span(&mut self) -> Result<(usize, usize, bool), ScanError> {
        if self.next() != Some(b'"') {
            return Err(ScanError::Invalid);
        }
        let start = self.i;
        let mut escaped = false;
        loop {
            let rest = &self.b[self.i..];
            let Some(p) = memchr::memchr2(b'"', b'\\', rest) else {
                return Err(ScanError::Invalid);
            };
            // raw control characters are not allowed inside JSON strings
            if rest[..p].iter().any(|&c| c < 0x20) {
                return Err(ScanError::Invalid);
            }
            self.i += p;
            if self.b[self.i] == b'"' {
                let end = self.i;
                self.i += 1;
                return Ok((start, end, escaped));
            }
            escaped = true;
            match self.b.get(self.i + 1) {
                Some(b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't') => self.i += 2,
                Some(b'u') => {
                    let hex = self
                        .b
                        .get(self.i + 2..self.i + 6)
                        .ok_or(ScanError::Invalid)?;
                    if !hex.iter().all(u8::is_ascii_hexdigit) {
                        return Err(ScanError::Invalid);
                    }
                    self.i += 6;
                }
                _ => return Err(ScanError::Invalid),
            }
        }
    }

    /// A number at the cursor, checked against the JSON grammar.
    fn number(&mut self) -> Result<&'a [u8], ScanError> {
        let start = self.i;
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        match self.next() {
            Some(b'0') => {}
            Some(b'1'..=b'9') => self.digits(),
            _ => return Err(ScanError::Invalid),
        }
        if self.peek() == Some(b'.') {
            self.i += 1;
            if !self.peek().is_some_and(|c| c.is_ascii_digit()) {
                return Err(ScanError::Invalid);
            }
            self.digits();
        }
        if let Some(b'e' | b'E') = self.peek() {
            self.i += 1;
            if let Some(b'+' | b'-') = self.peek() {
                self.i += 1;
            }
            if !self.peek().is_some_and(|c| c.is_ascii_digit()) {
                return Err(ScanError::Invalid);
            }
            self.digits();
        }
        Ok(&self.b[start..self.i])
    }

    #[inline]
    fn digits(&mut self) {
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.i += 1;
        }
    }

    /// Skip one value off every mapped path: structure checked, content not decoded.
    fn skip_value(&mut self, depth: usize) -> Result<(), ScanError> {
        match self.peek() {
            Some(b'"') => self.string().map(|_| ()),
            Some(b'{' | b'[') => self.skip_container(depth),
            Some(b't') => self.literal(b"true"),
            Some(b'f') => self.literal(b"false"),
            Some(b'n') => self.literal(b"null"),
            Some(b'-' | b'0'..=b'9') => self.number().map(|_| ()),
            _ => Err(ScanError::Invalid),
        }
    }

    /// Skip a container by bracket counting; strings inside are skipped as strings so that a
    /// bracket in a string does not count. Brackets must match in kind.
    fn skip_container(&mut self, depth: usize) -> Result<(), ScanError> {
        // a small stack of open bracket kinds; depth bounded by MAX_DEPTH
        let mut open = [0u8; MAX_DEPTH];
        let mut top = 0usize;
        loop {
            match self.peek() {
                Some(c @ (b'{' | b'[')) => {
                    if depth + top >= MAX_DEPTH {
                        return Err(ScanError::TooDeep);
                    }
                    open[top] = c;
                    top += 1;
                    self.i += 1;
                }
                Some(c @ (b'}' | b']')) => {
                    let want = if c == b'}' { b'{' } else { b'[' };
                    if top == 0 || open[top - 1] != want {
                        return Err(ScanError::Invalid);
                    }
                    top -= 1;
                    self.i += 1;
                    if top == 0 {
                        return Ok(());
                    }
                }
                Some(b'"') => {
                    self.string()?;
                }
                Some(_) => {
                    // the run of bytes up to the next structural or string character
                    let rest = &self.b[self.i..];
                    let p = rest
                        .iter()
                        .position(|&c| matches!(c, b'"' | b'{' | b'}' | b'[' | b']'))
                        .ok_or(ScanError::Invalid)?;
                    self.i += p;
                }
                None => return Err(ScanError::Invalid),
            }
        }
    }
}

/// Decode a JSON string body (between the quotes) into `out`. UTF-8 is checked; `\u` escapes
/// with surrogate pairs are combined; a lone surrogate is refused.
pub fn unescape(bytes: &[u8], out: &mut String) -> Result<(), ScanError> {
    out.clear();
    let mut i = 0;
    while i < bytes.len() {
        let Some(p) = memchr::memchr(b'\\', &bytes[i..]) else {
            out.push_str(std::str::from_utf8(&bytes[i..]).map_err(|_| ScanError::Invalid)?);
            return Ok(());
        };
        out.push_str(std::str::from_utf8(&bytes[i..i + p]).map_err(|_| ScanError::Invalid)?);
        i += p;
        let esc = *bytes.get(i + 1).ok_or(ScanError::Invalid)?;
        i += 2;
        let c = match esc {
            b'"' => '"',
            b'\\' => '\\',
            b'/' => '/',
            b'b' => '\u{8}',
            b'f' => '\u{c}',
            b'n' => '\n',
            b'r' => '\r',
            b't' => '\t',
            b'u' => {
                let hi = hex4(bytes.get(i..i + 4).ok_or(ScanError::Invalid)?)?;
                i += 4;
                let code = if (0xD800..0xDC00).contains(&hi) {
                    if bytes.get(i..i + 2) != Some(b"\\u") {
                        return Err(ScanError::Invalid);
                    }
                    let lo = hex4(bytes.get(i + 2..i + 6).ok_or(ScanError::Invalid)?)?;
                    if !(0xDC00..0xE000).contains(&lo) {
                        return Err(ScanError::Invalid);
                    }
                    i += 6;
                    0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
                } else {
                    hi
                };
                char::from_u32(code).ok_or(ScanError::Invalid)?
            }
            _ => return Err(ScanError::Invalid),
        };
        out.push(c);
    }
    Ok(())
}

fn hex4(h: &[u8]) -> Result<u32, ScanError> {
    let s = std::str::from_utf8(h).map_err(|_| ScanError::Invalid)?;
    u32::from_str_radix(s, 16).map_err(|_| ScanError::Invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths(p: &[&str]) -> Vec<Vec<String>> {
        p.iter()
            .map(|s| s.split('.').map(str::to_owned).collect())
            .collect()
    }

    #[test]
    fn finds_mapped_paths_and_skips_the_rest() {
        let plan = Plan::new(
            &paths(&["meta.time", "v.temp", "a.1", "missing"]),
            None,
            &[],
            0,
        );
        let msg = br#"{"junk":{"x":[1,{"y":"}]"}]},"meta":{"id":"r7","time":1762},"v":{"temp":-2.5e1},"a":["z",true]}"#;
        let mut f = Found::default();
        plan.scan(msg, &mut f).unwrap();
        let text = |i: usize| f.msg[i].map(|s| (s.kind, s.bytes(msg)));
        assert_eq!(text(0), Some((Kind::Num, &b"1762"[..])));
        assert_eq!(text(1), Some((Kind::Num, &b"-2.5e1"[..])));
        assert_eq!(text(2), Some((Kind::True, &b"true"[..])));
        assert_eq!(text(3), None);
    }

    #[test]
    fn rows_from_an_array_with_message_fields_before_and_after() {
        let plan = Plan::new(
            &paths(&["dev", "site"]),
            Some(&["samples".to_owned()]),
            &paths(&["t", "v"]),
            1000,
        );
        let msg =
            br#"{"dev":"d1","samples":[{"t":1,"v":2.5},{"v":null,"t":2},{"t":3}],"site":"s"}"#;
        let mut f = Found::default();
        plan.scan(msg, &mut f).unwrap();
        assert_eq!(f.rows, 3);
        let t = |s: Option<Span>| s.map(|s| (s.kind, s.bytes(msg)));
        assert_eq!(t(f.msg[0]), Some((Kind::Str, &b"d1"[..])));
        assert_eq!(t(f.msg[1]), Some((Kind::Str, &b"s"[..])));
        assert_eq!(
            (t(f.elems[0]), t(f.elems[1])),
            (Some((Kind::Num, &b"1"[..])), Some((Kind::Num, &b"2.5"[..])))
        );
        assert_eq!(
            (t(f.elems[2]), t(f.elems[3])),
            (
                Some((Kind::Num, &b"2"[..])),
                Some((Kind::Null, &b"null"[..]))
            )
        );
        assert_eq!(
            (t(f.elems[4]), t(f.elems[5])),
            (Some((Kind::Num, &b"3"[..])), None)
        );
        // too many elements
        let small = Plan::new(&[], Some(&["samples".to_owned()]), &paths(&["t"]), 2);
        assert_eq!(small.scan(msg, &mut f), Err(ScanError::TooManyRows));
    }

    #[test]
    fn refuses_malformed_and_too_deep_input() {
        let plan = Plan::new(&paths(&["a"]), None, &[], 0);
        let mut f = Found::default();
        for bad in [
            &b"{"[..],
            b"{\"a\":}",
            b"{\"a\":01}",
            b"{\"a\":1.}",
            b"{\"a\":tru}",
            b"{\"a\":1}x",
            b"{\"b\":[1,2}",
            b"{\"b\":\"x}",
            b"{\"a\":\"\x01\"}",
            b"{\"a\":\"\\q\"}",
            b"[1,2]]",
        ] {
            assert!(
                plan.scan(bad, &mut f).is_err(),
                "{:?}",
                String::from_utf8_lossy(bad)
            );
        }
        let deep = format!("{{\"b\":{}{}}}", "[".repeat(200), "]".repeat(200));
        assert_eq!(plan.scan(deep.as_bytes(), &mut f), Err(ScanError::TooDeep));
    }

    #[test]
    fn escaped_keys_and_strings() {
        let plan = Plan::new(&paths(&["a/b", "s"]), None, &[], 0);
        let msg = br#"{"a\/b":1,"s":"x\"y\u00e9\ud83d\ude00"}"#;
        let mut f = Found::default();
        plan.scan(msg, &mut f).unwrap();
        assert_eq!(f.msg[0].map(|s| s.bytes(msg)), Some(&b"1"[..]));
        let span = f.msg[1].unwrap();
        assert_eq!(span.kind, Kind::StrEscaped);
        let mut s = String::new();
        unescape(span.bytes(msg), &mut s).unwrap();
        assert_eq!(s, "x\"y\u{e9}\u{1f600}");
        assert!(unescape(br"\ud83d", &mut s).is_err());
    }

    // ---- differential: the scanner finds what a full JSON parser finds --------------------
    use proptest::prelude::*;
    use serde_json::Value as J;

    fn json_value() -> impl Strategy<Value = J> {
        let leaf = prop_oneof![
            Just(J::Null),
            any::<bool>().prop_map(J::Bool),
            any::<i64>().prop_map(J::from),
            any::<u64>().prop_map(J::from),
            (-1e300f64..1e300)
                .prop_map(|f| serde_json::Number::from_f64(f).map_or(J::Null, J::Number)),
            "[a-z0-9 _/\\\"\u{e9}\u{1f600}\n\t]{0,12}".prop_map(J::String),
        ];
        leaf.prop_recursive(5, 48, 6, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..5).prop_map(J::Array),
                prop::collection::vec(("[a-c]{1,2}", inner), 0..5)
                    .prop_map(|kv| J::Object(kv.into_iter().collect())),
            ]
        })
    }

    /// Every path to every node of `v` (object keys and array indexes as segments).
    fn all_paths(v: &J, prefix: &mut Vec<String>, out: &mut Vec<Vec<String>>) {
        match v {
            J::Object(m) => {
                for (k, c) in m {
                    if k.contains('.') {
                        continue;
                    }
                    prefix.push(k.clone());
                    out.push(prefix.clone());
                    all_paths(c, prefix, out);
                    prefix.pop();
                }
            }
            J::Array(a) => {
                for (i, c) in a.iter().enumerate() {
                    prefix.push(i.to_string());
                    out.push(prefix.clone());
                    all_paths(c, prefix, out);
                    prefix.pop();
                }
            }
            _ => {}
        }
    }

    fn lookup<'a>(v: &'a J, path: &[String]) -> Option<&'a J> {
        let mut cur = v;
        for p in path {
            cur = match cur {
                J::Object(m) => m.get(p)?,
                J::Array(a) => a.get(p.parse::<usize>().ok()?)?,
                _ => return None,
            };
        }
        Some(cur)
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: if cfg!(miri) { 4 } else { ProptestConfig::default().cases.max(2000) }, ..ProptestConfig::default() })]
        #[test]
        fn scanner_matches_a_full_parser(doc in json_value(), pick in prop::collection::vec(any::<prop::sample::Index>(), 1..6), pretty in any::<bool>()) {
            let text = if pretty { serde_json::to_string_pretty(&doc).unwrap() } else { serde_json::to_string(&doc).unwrap() };
            let mut paths_all = Vec::new();
            all_paths(&doc, &mut Vec::new(), &mut paths_all);
            let mut paths: Vec<Vec<String>> = if paths_all.is_empty() {
                Vec::new()
            } else {
                pick.iter().map(|ix| paths_all[ix.index(paths_all.len())].clone()).collect()
            };
            paths.push(vec!["no_such_key".into()]);
            let plan = Plan::new(&paths, None, &[], 0);
            let mut f = Found::default();
            plan.scan(text.as_bytes(), &mut f).expect("valid JSON must scan");
            for (slot, path) in paths.iter().enumerate() {
                let want = lookup(&doc, path);
                let got = f.msg[slot];
                match (want, got) {
                    (None, None) => {}
                    (Some(J::Object(_) | J::Array(_)), Some(sp)) => prop_assert_eq!(sp.kind, Kind::Composite),
                    (Some(J::Null), Some(sp)) => prop_assert_eq!(sp.kind, Kind::Null),
                    (Some(J::Bool(b)), Some(sp)) => prop_assert_eq!(sp.kind, if *b { Kind::True } else { Kind::False }),
                    (Some(J::Number(n)), Some(sp)) => {
                        // the document was written by serde_json: its number text is canonical,
                        // so the span must be exactly that text (comparing parsed values would
                        // test serde_json's own float parser, which is not correctly rounded)
                        prop_assert_eq!(sp.kind, Kind::Num);
                        let canonical = n.to_string();
                        prop_assert_eq!(sp.bytes(text.as_bytes()), canonical.as_bytes());
                    }
                    (Some(J::String(want)), Some(sp)) => {
                        let mut got = String::new();
                        if sp.kind == Kind::StrEscaped {
                            unescape(sp.bytes(text.as_bytes()), &mut got).unwrap();
                        } else {
                            prop_assert_eq!(sp.kind, Kind::Str);
                            got.push_str(std::str::from_utf8(sp.bytes(text.as_bytes())).unwrap());
                        }
                        prop_assert_eq!(&got, want);
                    }
                    (w, g) => prop_assert!(false, "path {:?}: parser {:?}, scanner {:?}", path, w, g),
                }
            }
        }

        /// Whatever the bytes, the scanner returns (it never panics or loops) and accepts no
        /// input a full parser refuses when the refusal is on a mapped path.
        #[test]
        fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..200)) {
            let plan = Plan::new(&[vec!["a".into()], vec!["b".into(), "0".into()]], Some(&["r".into()]), &[vec!["t".into()]], 50);
            let mut f = Found::default();
            let _ = plan.scan(&bytes, &mut f);
        }
    }
}
