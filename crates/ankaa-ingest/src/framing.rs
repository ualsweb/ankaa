//! Generic framing for user-defined binary feeds (PRD F-03).
//!
//! Most proprietary market-data protocols are *length-prefixed*: a small fixed header carries the
//! payload length, sometimes followed by a message-type byte. [`LengthPrefixed`] turns an
//! arbitrary byte stream (TCP segments, file chunks) into whole frames without copying payload
//! bytes out of the input buffer; a [`Decoder`](crate::Decoder) then turns each frame into events.
//!
//! ```
//! use ankaa_ingest::framing::{Framer, LengthPrefixed, Prefix};
//! // 2-byte big-endian length that counts only the payload
//! let mut f = Framer::new(LengthPrefixed::new(Prefix::U16Be).payload_only(), 1 << 20);
//! f.push(&[0, 3, b'a', b'b', b'c', 0, 1]);
//! assert_eq!(f.next_frame().as_deref(), Some(&b"abc"[..]));
//! assert!(f.next_frame().is_none()); // "0, 1" is a partial header
//! f.push(&[b'z']);
//! assert_eq!(f.next_frame().as_deref(), Some(&b"z"[..]));
//! ```

use std::ops::Range;

/// Width and byte order of the length field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Prefix {
    /// `u8`.
    U8,
    /// `u16` little-endian.
    U16Le,
    /// `u16` big-endian.
    U16Be,
    /// `u32` little-endian.
    U32Le,
    /// `u32` big-endian.
    U32Be,
}

impl Prefix {
    /// Bytes occupied by the length field.
    #[must_use]
    pub const fn width(self) -> usize {
        match self {
            Self::U8 => 1,
            Self::U16Le | Self::U16Be => 2,
            Self::U32Le | Self::U32Be => 4,
        }
    }

    fn read(self, b: &[u8]) -> usize {
        match self {
            Self::U8 => b[0] as usize,
            Self::U16Le => u16::from_le_bytes([b[0], b[1]]) as usize,
            Self::U16Be => u16::from_be_bytes([b[0], b[1]]) as usize,
            Self::U32Le => u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize,
            Self::U32Be => u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize,
        }
    }
}

/// Length-prefixed framing description.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LengthPrefixed {
    /// Length field.
    pub prefix: Prefix,
    /// Bytes before the length field (e.g. a magic or version byte).
    pub offset: usize,
    /// Header bytes after the length field that belong to the frame but are not counted by it.
    pub trailer_header: usize,
    /// `true` when the length counts the whole frame including the header; `false` when it counts
    /// only the bytes after the header.
    pub length_includes_header: bool,
    /// Strip the header from the frame handed to the decoder.
    pub strip_header: bool,
}

impl LengthPrefixed {
    /// Length at offset 0, counts the whole frame, header kept (SBE-like protocols put the
    /// template id after the length and decoders want to see it).
    #[must_use]
    pub const fn new(prefix: Prefix) -> Self {
        Self {
            prefix,
            offset: 0,
            trailer_header: 0,
            length_includes_header: true,
            strip_header: false,
        }
    }

    /// Length counts only the payload; the header is stripped from the yielded frame.
    #[must_use]
    pub const fn payload_only(mut self) -> Self {
        self.length_includes_header = false;
        self.strip_header = true;
        self
    }

    /// Bytes before the length field.
    #[must_use]
    pub const fn with_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    /// Extra header bytes after the length field (not counted by it when `payload_only`).
    #[must_use]
    pub const fn with_trailer_header(mut self, n: usize) -> Self {
        self.trailer_header = n;
        self
    }

    /// Total header length.
    #[must_use]
    pub const fn header_len(&self) -> usize {
        self.offset + self.prefix.width() + self.trailer_header
    }

    /// Frame geometry for the header at the start of `buf`: `(frame_len, payload_range)`, or
    /// `Ok(None)` when more bytes are needed, `Err(FrameError)` when the header is invalid.
    fn parse(
        &self,
        buf: &[u8],
        max_frame: usize,
    ) -> Result<Option<(usize, Range<usize>)>, FrameError> {
        let hl = self.header_len();
        if buf.len() < hl {
            return Ok(None);
        }
        let n = self.prefix.read(&buf[self.offset..]);
        let frame_len = if self.length_includes_header {
            if n < hl {
                return Err(FrameError::LengthBelowHeader {
                    length: n,
                    header: hl,
                });
            }
            n
        } else {
            n.saturating_add(hl)
        };
        if frame_len > max_frame {
            return Err(FrameError::TooLong {
                length: frame_len,
                max: max_frame,
            });
        }
        if buf.len() < frame_len {
            return Ok(None);
        }
        let range = if self.strip_header {
            hl..frame_len
        } else {
            0..frame_len
        };
        Ok(Some((frame_len, range)))
    }
}

/// Framing error: the stream is desynchronised and the caller must resync (reconnect or seek).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FrameError {
    /// Declared length is shorter than the header it includes.
    #[error("declared length {length} is below the header length {header}")]
    LengthBelowHeader {
        /// Declared length.
        length: usize,
        /// Header length.
        header: usize,
    },
    /// Declared length exceeds the configured maximum (bounds memory on hostile input).
    #[error("frame of {length} bytes exceeds the maximum of {max}")]
    TooLong {
        /// Declared frame length.
        length: usize,
        /// Configured maximum.
        max: usize,
    },
}

/// Incremental framer over a byte stream. Payload bytes are never copied twice: `push` appends
/// to one internal buffer and `next_frame` hands out sub-slices of it, compacting lazily.
#[derive(Debug)]
pub struct Framer {
    layout: LengthPrefixed,
    max_frame: usize,
    buf: Vec<u8>,
    /// Start of unconsumed bytes in `buf`.
    head: usize,
    error: Option<FrameError>,
    frames: u64,
    bytes: u64,
}

impl Framer {
    /// New framer; `max_frame` bounds the buffer on hostile input.
    #[must_use]
    pub fn new(layout: LengthPrefixed, max_frame: usize) -> Self {
        Self {
            layout,
            max_frame,
            buf: Vec::with_capacity(64 * 1024),
            head: 0,
            error: None,
            frames: 0,
            bytes: 0,
        }
    }

    /// Append stream bytes. After a [`FrameError`] the bytes are dropped (still counted in
    /// [`Framer::bytes`]) until [`Framer::reset`], so a desynchronised stream cannot grow memory.
    pub fn push(&mut self, bytes: &[u8]) {
        self.bytes += bytes.len() as u64;
        if self.error.is_some() {
            return;
        }
        if self.head > 0 && self.head * 2 >= self.buf.len() {
            self.buf.drain(..self.head);
            self.head = 0;
        }
        self.buf.extend_from_slice(bytes);
    }

    /// Next complete frame, if any. After a [`FrameError`] the framer yields nothing until
    /// [`Framer::reset`]; check [`Framer::error`].
    pub fn next_frame(&mut self) -> Option<&[u8]> {
        if self.error.is_some() {
            return None;
        }
        let rest = &self.buf[self.head..];
        match self.layout.parse(rest, self.max_frame) {
            Ok(Some((len, range))) => {
                let start = self.head;
                self.head += len;
                self.frames += 1;
                Some(&self.buf[start + range.start..start + range.end])
            }
            Ok(None) => None,
            Err(e) => {
                self.error = Some(e);
                None
            }
        }
    }

    /// Pending framing error (stream desynchronised).
    #[must_use]
    pub fn error(&self) -> Option<&FrameError> {
        self.error.as_ref()
    }

    /// Drop buffered bytes and clear the error (after a reconnect).
    pub fn reset(&mut self) {
        self.buf.clear();
        self.head = 0;
        self.error = None;
    }

    /// Unconsumed bytes waiting for the rest of a frame.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.buf.len() - self.head
    }

    /// Frames yielded so far.
    #[must_use]
    pub fn frames(&self) -> u64 {
        self.frames
    }

    /// Bytes pushed so far.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

/// Split a complete in-memory buffer (a file) into frames; stops at the first error or partial
/// frame and returns how many bytes were consumed.
pub fn split_all<'a>(
    layout: LengthPrefixed,
    max_frame: usize,
    mut buf: &'a [u8],
    mut f: impl FnMut(&'a [u8]),
) -> Result<usize, FrameError> {
    let total = buf.len();
    loop {
        match layout.parse(buf, max_frame)? {
            Some((len, range)) => {
                f(&buf[range]);
                buf = &buf[len..];
            }
            None => return Ok(total - buf.len()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn encode(layout: LengthPrefixed, payloads: &[Vec<u8>]) -> Vec<u8> {
        let mut out = Vec::new();
        for p in payloads {
            let hl = layout.header_len();
            let n = if layout.length_includes_header {
                p.len() + hl
            } else {
                p.len()
            };
            out.extend(std::iter::repeat_n(0xAA, layout.offset));
            match layout.prefix {
                Prefix::U8 => out.push(u8::try_from(n).unwrap()),
                Prefix::U16Le => out.extend(u16::try_from(n).unwrap().to_le_bytes()),
                Prefix::U16Be => out.extend(u16::try_from(n).unwrap().to_be_bytes()),
                Prefix::U32Le => out.extend(u32::try_from(n).unwrap().to_le_bytes()),
                Prefix::U32Be => out.extend(u32::try_from(n).unwrap().to_be_bytes()),
            }
            out.extend(std::iter::repeat_n(0xBB, layout.trailer_header));
            out.extend(p);
        }
        out
    }

    fn layouts() -> impl Strategy<Value = LengthPrefixed> {
        (
            prop_oneof![
                Just(Prefix::U8),
                Just(Prefix::U16Le),
                Just(Prefix::U16Be),
                Just(Prefix::U32Le),
                Just(Prefix::U32Be)
            ],
            0usize..3,
            0usize..3,
            any::<bool>(),
        )
            .prop_map(|(prefix, offset, trailer, payload_only)| {
                let l = LengthPrefixed::new(prefix)
                    .with_offset(offset)
                    .with_trailer_header(trailer);
                if payload_only { l.payload_only() } else { l }
            })
    }

    proptest! {
        #[test]
        fn any_split_of_the_stream_yields_the_same_frames(
            layout in layouts(),
            payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..40), 0..20),
            cuts in prop::collection::vec(0usize..64, 0..40),
        ) {
            let stream = encode(layout, &payloads);
            let mut f = Framer::new(layout, 1 << 16);
            let mut got: Vec<Vec<u8>> = Vec::new();
            let mut pos: usize = 0;
            for c in cuts.iter().chain(std::iter::once(&usize::MAX)) {
                let end = pos.saturating_add(*c).min(stream.len());
                f.push(&stream[pos..end]);
                pos = end;
                while let Some(fr) = f.next_frame() {
                    got.push(fr.to_vec());
                }
                if pos == stream.len() { break; }
            }
            prop_assert!(f.error().is_none());
            prop_assert_eq!(f.pending(), 0);
            let expect: Vec<Vec<u8>> = payloads.iter().map(|p| {
                if layout.strip_header { p.clone() } else {
                    encode(layout, std::slice::from_ref(p))
                }
            }).collect();
            prop_assert_eq!(got, expect);
            let mut n = 0;
            let consumed = split_all(layout, 1 << 16, &stream, |_| n += 1).unwrap();
            prop_assert_eq!(consumed, stream.len());
            prop_assert_eq!(n, payloads.len());
        }

        #[test]
        fn never_panics_on_garbage(bytes in prop::collection::vec(any::<u8>(), 0..300), layout in layouts()) {
            let mut f = Framer::new(layout, 128);
            f.push(&bytes);
            while f.next_frame().is_some() {}
            let _ = split_all(layout, 128, &bytes, |_| {});
        }
    }

    #[test]
    fn too_long_and_below_header_are_errors() {
        let mut f = Framer::new(LengthPrefixed::new(Prefix::U16Be), 10);
        f.push(&[0, 11]);
        assert!(f.next_frame().is_none());
        assert_eq!(
            f.error(),
            Some(&FrameError::TooLong {
                length: 11,
                max: 10
            })
        );
        f.reset();
        f.push(&[0, 1, 0]);
        assert!(f.next_frame().is_none());
        assert!(matches!(
            f.error(),
            Some(FrameError::LengthBelowHeader { .. })
        ));
    }
}
