// Protobuf wire encoding — bounded, `no_std`, no-alloc, `forbid(unsafe_code)`
// compatible. Writes into a caller-provided buffer, so a device can seal a
// canonical artefact without `prost` and without an allocator.
//
// `prost` needs `std` + alloc, so it cannot run on a bare-metal node. The
// encoding is not the hard part: a message is a tag varint followed by either a
// varint or a length-delimited region — the shape `ser_core` already frames,
// here behind a direct writer API, because artefact builders construct messages
// programmatically rather than from a bytecode program.
//
// BYTE-IDENTITY IS THE CONTRACT, not an aspiration. If this encoder and `prost`
// disagree by one byte, the same artefact gets two digests and every pin,
// signature and content-address built on it silently splits in half. The rules
// that keep them equal, all proto3:
//
//   * minimal varints — never a padded length slot;
//   * ascending field order — prost writes fields in tag order, so callers must
//     too (this writer does not reorder for you);
//   * default values are ABSENT — a `0` / `""` / empty-bytes / `false` scalar
//     emits nothing, which is why the `*_field` helpers skip rather than write;
//   * a MESSAGE field is presence-based — `Some(empty)` still emits a tag and a
//     zero length, so `open`/`close` always write, and the caller decides.
//
// `tests/harness/tests/chronicle_cli.rs (corpus suite)` pins this against prost over
// the real artefacts; treat a diff there as an identity break, never a nit.

/// Deterministic encoding failures — values, never panics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PbError {
    /// The output buffer cannot hold the message.
    Overflow,
    /// More nested messages open than `PB_MAX_DEPTH`.
    TooDeep,
    /// A message being READ ended mid-field, or declared a length past its end.
    Truncated,
    /// A wire type this reader does not handle.
    BadWire(u32),
}

/// Nesting cap. Artefact schemas nest a handful deep; a bound makes the
/// writer's own recursion-free operation provable.
pub const PB_MAX_DEPTH: usize = 16;

/// Wire types (proto3 uses 0, 1, 2 and 5; artefacts use 0 and 2).
pub const WT_VARINT: u32 = 0;
pub const WT_LEN: u32 = 2;

/// An open length-delimited region, returned by [`Pb::open`] and consumed by
/// [`Pb::close`]. Carries the content start so closing can measure and prefix.
#[derive(Debug, Clone, Copy)]
pub struct PbMark {
    start: usize,
}

/// A protobuf writer over a caller buffer.
pub struct Pb<'a> {
    buf: &'a mut [u8],
    pos: usize,
    depth: usize,
}

impl<'a> Pb<'a> {
    pub fn new(buf: &'a mut [u8]) -> Self {
        Pb {
            buf,
            pos: 0,
            depth: 0,
        }
    }

    /// Bytes written so far.
    pub fn len(&self) -> usize {
        self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.pos == 0
    }

    fn put(&mut self, b: u8) -> Result<(), PbError> {
        if self.pos >= self.buf.len() {
            return Err(PbError::Overflow);
        }
        self.buf[self.pos] = b;
        self.pos += 1;
        Ok(())
    }

    fn put_slice(&mut self, s: &[u8]) -> Result<(), PbError> {
        if self.pos + s.len() > self.buf.len() {
            return Err(PbError::Overflow);
        }
        self.buf[self.pos..self.pos + s.len()].copy_from_slice(s);
        self.pos += s.len();
        Ok(())
    }

    /// Base-128 varint, minimal width — the only width prost emits.
    pub fn varint(&mut self, mut v: u64) -> Result<(), PbError> {
        while v >= 0x80 {
            self.put((v as u8) | 0x80)?;
            v >>= 7;
        }
        self.put(v as u8)
    }

    /// `(field << 3) | wire_type`.
    pub fn tag(&mut self, field: u32, wire: u32) -> Result<(), PbError> {
        self.varint(((field as u64) << 3) | (wire as u64))
    }

    // ---- scalar fields: proto3 skips defaults, so these skip too -----------

    pub fn u64_field(&mut self, field: u32, v: u64) -> Result<(), PbError> {
        if v == 0 {
            return Ok(());
        }
        self.tag(field, WT_VARINT)?;
        self.varint(v)
    }

    /// proto3 `int32`/`enum`: negatives are sign-extended to 10 bytes, which is
    /// what prost does — not zig-zag (that is `sint32`).
    pub fn i32_field(&mut self, field: u32, v: i32) -> Result<(), PbError> {
        if v == 0 {
            return Ok(());
        }
        self.tag(field, WT_VARINT)?;
        self.varint(v as i64 as u64)
    }

    pub fn i64_field(&mut self, field: u32, v: i64) -> Result<(), PbError> {
        if v == 0 {
            return Ok(());
        }
        self.tag(field, WT_VARINT)?;
        self.varint(v as u64)
    }

    pub fn bool_field(&mut self, field: u32, v: bool) -> Result<(), PbError> {
        if !v {
            return Ok(());
        }
        self.tag(field, WT_VARINT)?;
        self.varint(1)
    }

    /// `string` / `bytes`. Empty is the proto3 default and emits nothing.
    pub fn bytes_field(&mut self, field: u32, v: &[u8]) -> Result<(), PbError> {
        if v.is_empty() {
            return Ok(());
        }
        self.tag(field, WT_LEN)?;
        self.varint(v.len() as u64)?;
        self.put_slice(v)
    }

    /// A `string`/`bytes` element of a REPEATED field: written even when empty,
    /// because a present element is not a default.
    pub fn bytes_elem(&mut self, field: u32, v: &[u8]) -> Result<(), PbError> {
        self.tag(field, WT_LEN)?;
        self.varint(v.len() as u64)?;
        self.put_slice(v)
    }

    // ---- nested messages --------------------------------------------------

    /// Open a length-delimited field. The length is unknown until the content
    /// is written, so it is inserted by [`Pb::close`] — never reserved, since a
    /// padded varint would diverge from prost.
    pub fn open(&mut self, field: u32) -> Result<PbMark, PbError> {
        if self.depth >= PB_MAX_DEPTH {
            return Err(PbError::TooDeep);
        }
        self.tag(field, WT_LEN)?;
        self.depth += 1;
        Ok(PbMark { start: self.pos })
    }

    /// Close the region opened by `mark`: measure the content, shift it right
    /// by the width of its (minimal) varint length, and write that length in
    /// the gap. Same technique as `ser_core`'s `RGN_VARINT`, and the shift is a
    /// manual backward loop rather than `copy_within` to keep the freestanding
    /// build free of a formatted-panic path.
    pub fn close(&mut self, mark: PbMark) -> Result<(), PbError> {
        let content_len = self.pos - mark.start;
        let width = varint_len(content_len as u64);
        if self.pos + width > self.buf.len() {
            return Err(PbError::Overflow);
        }
        // Shift [start..pos) right by `width`, back to front so it cannot
        // overwrite bytes it has yet to move.
        let mut i = self.pos;
        while i > mark.start {
            i -= 1;
            self.buf[i + width] = self.buf[i];
        }
        // Write the length into the gap.
        let mut v = content_len as u64;
        let mut w = mark.start;
        while v >= 0x80 {
            self.buf[w] = (v as u8) | 0x80;
            v >>= 7;
            w += 1;
        }
        self.buf[w] = v as u8;
        self.pos += width;
        self.depth -= 1;
        Ok(())
    }

    /// An empty message field: tag plus a zero length. Presence, not content.
    pub fn empty_msg(&mut self, field: u32) -> Result<(), PbError> {
        self.tag(field, WT_LEN)?;
        self.varint(0)
    }
}

/// Bytes a minimal varint of `v` occupies.
pub fn varint_len(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

// ------------------------------------------------------------------ reading
//
// The decoding half. Verifying a signature on device means RECOMPUTING what was
// signed, which means taking the received bytes apart — so the writer alone is
// not enough. This reader borrows rather than copies: every field points into
// the caller's buffer, so walking a message allocates nothing.

/// One field as it appears on the wire.
#[derive(Debug, Clone, Copy)]
pub struct PbField<'a> {
    pub number: u32,
    pub wire: u32,
    /// Payload for a length-delimited field; empty otherwise.
    pub bytes: &'a [u8],
    /// Value for a varint field; 0 otherwise.
    pub value: u64,
    /// The COMPLETE encoded field — tag, length, payload. This is what lets a
    /// filter copy a field through byte-for-byte instead of re-encoding it, so
    /// a re-serialization cannot perturb bytes it was only meant to pass on.
    pub raw: &'a [u8],
}

/// A forward cursor over a protobuf message.
pub struct PbR<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> PbR<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        PbR { buf, pos: 0 }
    }

    pub fn is_done(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn varint(&mut self) -> Result<u64, PbError> {
        let mut v = 0u64;
        let mut shift = 0u32;
        loop {
            if self.pos >= self.buf.len() || shift > 63 {
                return Err(PbError::Truncated);
            }
            let b = self.buf[self.pos];
            self.pos += 1;
            v |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(v);
            }
            shift += 7;
        }
    }

    /// The next field, or `None` at the end of the message.
    pub fn next_field(&mut self) -> Result<Option<PbField<'a>>, PbError> {
        if self.is_done() {
            return Ok(None);
        }
        let start = self.pos;
        let key = self.varint()?;
        let number = (key >> 3) as u32;
        let wire = (key & 7) as u32;
        let (bytes, value) = match wire {
            WT_VARINT => (&self.buf[..0], self.varint()?),
            WT_LEN => {
                let len = self.varint()? as usize;
                let end = self.pos.checked_add(len).ok_or(PbError::Truncated)?;
                if end > self.buf.len() {
                    return Err(PbError::Truncated);
                }
                let b = &self.buf[self.pos..end];
                self.pos = end;
                (b, 0u64)
            }
            other => return Err(PbError::BadWire(other)),
        };
        Ok(Some(PbField {
            number,
            wire,
            bytes,
            value,
            raw: &self.buf[start..self.pos],
        }))
    }
}

/// Write `src` verbatim. Used to pass an already-encoded field through a filter
/// without re-encoding it.
impl Pb<'_> {
    pub fn raw(&mut self, src: &[u8]) -> Result<(), PbError> {
        for &b in src {
            self.put(b)?;
        }
        Ok(())
    }
}
