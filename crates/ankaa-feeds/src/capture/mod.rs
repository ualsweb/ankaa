//! Raw-frame capture format (`.ankc`): the exact bytes a feed delivered, with receive
//! timestamps, so decoders can be developed, fuzzed and regression-tested offline and
//! ingestion can be replayed at any speed.
//!
//! File = header + frames. Little-endian.
//!
//! ```text
//! header  : magic "ANKC" (4) | version u16 | flags u16 | source_len u16 | source (utf8) | reserved (6)
//! frame   : recv_ns i64 | kind u8 | reserved u8 | len u32 | payload[len] | crc32c u32 (over recv_ns..payload)
//! ```
//!
//! `kind`: 1 = text (WebSocket text frame), 2 = binary, 3 = control (connect/disconnect/gap note),
//! 4 = snapshot (UTF-8 JSON `{"s":"<symbol>","snapshot":<REST depth body>}` fetched by the capture tool).

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// File magic.
pub const MAGIC: &[u8; 4] = b"ANKC";
/// Format version.
pub const VERSION: u16 = 1;

/// Frame kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    /// WebSocket text payload.
    Text = 1,
    /// Binary payload.
    Binary = 2,
    /// Capture-tool control note (UTF-8), e.g. `connect`, `disconnect:<reason>`.
    Control = 3,
    /// Book snapshot fetched out of band (UTF-8 JSON with a symbol wrapper).
    Snapshot = 4,
    /// User-defined source (generic ingest): payload is `[u16 LE source id][message bytes]`.
    User = 5,
}

/// Build a [`FrameKind::User`] payload: source id prefix + message bytes.
#[must_use]
pub fn user_payload(source_id: u16, msg: &[u8]) -> Bytes {
    let mut v = Vec::with_capacity(2 + msg.len());
    v.extend_from_slice(&source_id.to_le_bytes());
    v.extend_from_slice(msg);
    Bytes::from(v)
}

/// Split a [`FrameKind::User`] payload into `(source id, message bytes)`.
#[must_use]
pub fn split_user_payload(payload: &[u8]) -> Option<(u16, &[u8])> {
    let id = u16::from_le_bytes([*payload.first()?, *payload.get(1)?]);
    Some((id, &payload[2..]))
}

/// Prefix of the control frame a source sends to record its position (see
/// [`source_state_payload`]).
pub const SOURCE_STATE_PREFIX: &[u8] = b"src-state:";

/// Build the payload of a [`FrameKind::Control`] frame that records how far a source has read:
/// `src-state:` + `[u16 LE source id]` + state text. The frame travels in band, after the
/// messages it covers, so whoever persists it knows those messages came first.
#[must_use]
pub fn source_state_payload(source_id: u16, state: &str) -> Bytes {
    let mut v = Vec::with_capacity(SOURCE_STATE_PREFIX.len() + 2 + state.len());
    v.extend_from_slice(SOURCE_STATE_PREFIX);
    v.extend_from_slice(&source_id.to_le_bytes());
    v.extend_from_slice(state.as_bytes());
    Bytes::from(v)
}

/// Split a source-state control payload into `(source id, state)`.
#[must_use]
pub fn split_source_state(payload: &[u8]) -> Option<(u16, &str)> {
    let rest = payload.strip_prefix(SOURCE_STATE_PREFIX)?;
    let id = u16::from_le_bytes([*rest.first()?, *rest.get(1)?]);
    Some((id, std::str::from_utf8(&rest[2..]).ok()?))
}

/// Prefix of the control frame that records which name a source id belongs to.
pub const SOURCE_ID_PREFIX: &[u8] = b"src-id:";

/// Payload of a [`FrameKind::Control`] frame binding a source id to its name: `src-id:` +
/// `[u16 LE id]` + name. A server writes one per source at start, so whoever replays its log
/// (recovery, a standby) maps user frames to the same sources whatever its own configuration
/// order is.
#[must_use]
pub fn source_id_payload(source_id: u16, name: &str) -> Bytes {
    let mut v = Vec::with_capacity(SOURCE_ID_PREFIX.len() + 2 + name.len());
    v.extend_from_slice(SOURCE_ID_PREFIX);
    v.extend_from_slice(&source_id.to_le_bytes());
    v.extend_from_slice(name.as_bytes());
    Bytes::from(v)
}

/// Split a source-id control payload into `(source id, name)`.
#[must_use]
pub fn split_source_id(payload: &[u8]) -> Option<(u16, &str)> {
    let rest = payload.strip_prefix(SOURCE_ID_PREFIX)?;
    let id = u16::from_le_bytes([*rest.first()?, *rest.get(1)?]);
    let name = std::str::from_utf8(&rest[2..]).ok()?;
    (!name.is_empty()).then_some((id, name))
}

impl FrameKind {
    fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Text),
            2 => Some(Self::Binary),
            3 => Some(Self::Control),
            4 => Some(Self::Snapshot),
            5 => Some(Self::User),
            _ => None,
        }
    }
}

/// One captured frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Receive time, nanoseconds since the Unix epoch (host clock at the capture tool).
    pub recv_ns: i64,
    /// Kind.
    pub kind: FrameKind,
    /// Payload bytes.
    pub payload: Bytes,
}

/// Decode errors.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CaptureError {
    /// Bad magic.
    #[error("not an ANKC capture (bad magic)")]
    BadMagic,
    /// Unsupported version.
    #[error("unsupported capture version {0}")]
    Version(u16),
    /// Truncated input.
    #[error("truncated at offset {0}")]
    Truncated(usize),
    /// CRC mismatch.
    #[error("crc mismatch at offset {0}")]
    Crc(usize),
    /// Unknown frame kind byte.
    #[error("unknown frame kind {0}")]
    Kind(u8),
    /// Frame length above the sanity limit.
    #[error("frame length {0} exceeds limit")]
    TooLong(u32),
}

/// Maximum payload accepted on read (defensive; Binance depth frames are well under 1 MiB).
pub const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;

/// Encode the file header.
pub fn encode_header(source: &str, out: &mut BytesMut) {
    out.put_slice(MAGIC);
    out.put_u16_le(VERSION);
    out.put_u16_le(0);
    let s = source.as_bytes();
    let n = u16::try_from(s.len().min(u16::MAX as usize)).unwrap_or(u16::MAX);
    out.put_u16_le(n);
    out.put_slice(&s[..n as usize]);
    out.put_slice(&[0u8; 6]);
}

/// Encode one frame.
pub fn encode_frame<B: BufMut + std::ops::Deref<Target = [u8]>>(f: &Frame, out: &mut B) {
    let start = out.len();
    out.put_i64_le(f.recv_ns);
    out.put_u8(f.kind as u8);
    out.put_u8(0);
    out.put_u32_le(f.payload.len() as u32);
    out.put_slice(&f.payload);
    let crc = crc32c::crc32c(&out[start..]);
    out.put_u32_le(crc);
}

/// Parsed header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// Source label written by the capture tool (e.g. the WebSocket URL).
    pub source: String,
}

/// Decode the header; returns it and the number of bytes consumed.
pub fn decode_header(buf: &[u8]) -> Result<(Header, usize), CaptureError> {
    if buf.len() < 10 {
        return Err(CaptureError::Truncated(buf.len()));
    }
    if &buf[..4] != MAGIC {
        return Err(CaptureError::BadMagic);
    }
    let mut b = &buf[4..];
    let version = b.get_u16_le();
    if version != VERSION {
        return Err(CaptureError::Version(version));
    }
    let _flags = b.get_u16_le();
    let n = b.get_u16_le() as usize;
    if b.len() < n + 6 {
        return Err(CaptureError::Truncated(buf.len()));
    }
    let source = String::from_utf8_lossy(&b[..n]).into_owned();
    Ok((Header { source }, 10 + n + 6))
}

/// Decode one frame at `buf[0..]`; returns the frame and bytes consumed.
pub fn decode_frame(buf: &[u8], offset_for_errors: usize) -> Result<(Frame, usize), CaptureError> {
    const FIXED: usize = 8 + 1 + 1 + 4;
    if buf.len() < FIXED {
        return Err(CaptureError::Truncated(offset_for_errors));
    }
    let mut b = buf;
    let recv_ns = b.get_i64_le();
    let kind_b = b.get_u8();
    let _reserved = b.get_u8();
    let len = b.get_u32_le();
    if len > MAX_FRAME_LEN {
        return Err(CaptureError::TooLong(len));
    }
    let total = FIXED + len as usize + 4;
    if buf.len() < total {
        return Err(CaptureError::Truncated(offset_for_errors));
    }
    let c = &buf[total - 4..total];
    let crc_want = u32::from_le_bytes([c[0], c[1], c[2], c[3]]);
    let crc_got = crc32c::crc32c(&buf[..total - 4]);
    if crc_want != crc_got {
        return Err(CaptureError::Crc(offset_for_errors));
    }
    let kind = FrameKind::from_u8(kind_b).ok_or(CaptureError::Kind(kind_b))?;
    let payload = Bytes::copy_from_slice(&buf[FIXED..FIXED + len as usize]);
    Ok((
        Frame {
            recv_ns,
            kind,
            payload,
        },
        total,
    ))
}

/// Decode all frames of a capture held in a [`Bytes`] buffer **without copying payloads**
/// (each frame's payload is a slice of `buf`).
pub fn decode_all_bytes(buf: &Bytes) -> Result<(Header, Vec<Frame>), CaptureError> {
    const FIXED: usize = 8 + 1 + 1 + 4;
    let (h, mut off) = decode_header(buf)?;
    let mut frames = Vec::new();
    while off < buf.len() {
        let (mut f, used) = decode_frame(&buf[off..], off)?;
        f.payload = buf.slice(off + FIXED..off + used - 4);
        frames.push(f);
        off += used;
    }
    Ok((h, frames))
}

/// Like [`decode_all_bytes`], but a capture whose writer was interrupted is still usable: decoding
/// stops at the first frame that fails (truncated or bad CRC) and returns every frame before it,
/// with the error describing where the file stopped being valid. A bad header is still an error.
pub fn decode_all_bytes_tolerant(
    buf: &Bytes,
) -> Result<(Header, Vec<Frame>, Option<CaptureError>), CaptureError> {
    let (h, it) = FrameIter::new(buf.clone())?;
    let mut frames = Vec::new();
    let mut tail = None;
    for f in it {
        match f {
            Ok(f) => frames.push(f),
            Err(e) => tail = Some(e),
        }
    }
    Ok((h, frames, tail))
}

/// Streaming iterator over the frames of a capture held in a [`Bytes`] buffer (zero-copy payloads,
/// no intermediate `Vec<Frame>`). Stops at the first error, which is yielded once.
#[derive(Debug)]
pub struct FrameIter {
    buf: Bytes,
    off: usize,
    done: bool,
}

impl FrameIter {
    /// Parse the header and position at the first frame.
    pub fn new(buf: Bytes) -> Result<(Header, Self), CaptureError> {
        let (h, off) = decode_header(&buf)?;
        Ok((
            h,
            Self {
                buf,
                off,
                done: false,
            },
        ))
    }
}

impl Iterator for FrameIter {
    type Item = Result<Frame, CaptureError>;
    fn next(&mut self) -> Option<Self::Item> {
        const FIXED: usize = 8 + 1 + 1 + 4;
        if self.done || self.off >= self.buf.len() {
            return None;
        }
        match decode_frame(&self.buf[self.off..], self.off) {
            Ok((mut f, used)) => {
                f.payload = self.buf.slice(self.off + FIXED..self.off + used - 4);
                self.off += used;
                Some(Ok(f))
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

/// Iterate over all frames of a capture file held in memory (payloads copied).
pub fn decode_all(buf: &[u8]) -> Result<(Header, Vec<Frame>), CaptureError> {
    let (h, mut off) = decode_header(buf)?;
    let mut frames = Vec::new();
    while off < buf.len() {
        let (f, used) = decode_frame(&buf[off..], off)?;
        frames.push(f);
        off += used;
    }
    Ok((h, frames))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let mut out = BytesMut::new();
        encode_header("wss://example/ws", &mut out);
        let frames = vec![
            Frame {
                recv_ns: 1,
                kind: FrameKind::Control,
                payload: Bytes::from_static(b"connect"),
            },
            Frame {
                recv_ns: 2,
                kind: FrameKind::Text,
                payload: Bytes::from_static(br#"{"e":"trade"}"#),
            },
            Frame {
                recv_ns: 3,
                kind: FrameKind::Binary,
                payload: Bytes::from_static(&[0, 1, 2, 255]),
            },
        ];
        for f in &frames {
            encode_frame(f, &mut out);
        }
        let (h, got) = decode_all(&out).unwrap();
        assert_eq!(h.source, "wss://example/ws");
        assert_eq!(got, frames);
    }

    #[test]
    fn detects_corruption_and_truncation() {
        let mut out = BytesMut::new();
        encode_header("x", &mut out);
        encode_frame(
            &Frame {
                recv_ns: 9,
                kind: FrameKind::Text,
                payload: Bytes::from_static(b"hello"),
            },
            &mut out,
        );
        let mut bad = out.to_vec();
        let last = bad.len() - 6;
        bad[last] ^= 0x01;
        assert!(matches!(decode_all(&bad), Err(CaptureError::Crc(_))));
        let trunc = &out[..out.len() - 2];
        assert!(matches!(decode_all(trunc), Err(CaptureError::Truncated(_))));
        let (_, frames, tail) = decode_all_bytes_tolerant(&Bytes::copy_from_slice(trunc)).unwrap();
        assert!(matches!(tail, Some(CaptureError::Truncated(_))));
        // source-state control payloads round-trip and reject other control notes
        let p = source_state_payload(513, "0=12,1=40");
        assert_eq!(split_source_state(&p), Some((513, "0=12,1=40")));
        assert_eq!(split_source_state(b"connect"), None);
        let p = source_id_payload(7, "racks");
        assert_eq!(split_source_id(&p), Some((7, "racks")));
        assert_eq!(split_source_id(b"src-id:\x01\x00"), None);
        assert_eq!(split_source_state(&p), None);
        assert_eq!(split_source_state(b"src-state:\x01"), None);
        assert_eq!(frames.len() + 1, decode_all(&out).unwrap().1.len());
    }
}
