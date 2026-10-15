// Bounded, no_std, no-alloc receiver for a `blob` module param. Like the other
// `*_core.rs` files it has NO inner attributes and NO test module, so it is
// `include!`d verbatim by both this crate (`lib.rs`) and the param-driven
// Fluxor modules — one source of truth, host and device.
//
// A program (a decision container, an ir_stages container, a codec program)
// reaches its engine as a `blob` param: its own bytes, not their hex — hex
// doubled every program in a graph's config. fluxor's packer carries a blob
// as TLV entries of at most 255 bytes under the param's tag, the FIRST led by
// the whole blob's length (u16 LE); a graph writes it as `{hex: "…"}`, which
// the packer decodes before it is packed.

/// One blob param on its way in: the length its first chunk declared, and how
/// much of it has arrived.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BlobIn {
    /// The blob's whole length, from its first chunk.
    pub total: u16,
    /// Bytes copied so far.
    pub len: u16,
    /// A first chunk has been seen.
    pub started: bool,
    /// More arrived than the declared length or the buffer holds.
    pub overflow: bool,
}

impl BlobIn {
    pub const fn new() -> Self {
        BlobIn {
            total: 0,
            len: 0,
            started: false,
            overflow: false,
        }
    }

    /// Take one TLV chunk of the blob into `dst`, after what came before.
    /// An empty chunk is no chunk: `define_params!` hands every handler its
    /// default before the TLV, and a blob's default is zero bytes.
    pub fn take(&mut self, dst: &mut [u8], chunk: &[u8]) {
        if chunk.is_empty() {
            return;
        }
        let mut body = chunk;
        if !self.started {
            let (Some(&lo), Some(&hi)) = (chunk.first(), chunk.get(1)) else {
                self.overflow = true;
                return;
            };
            self.total = u16::from_le_bytes([lo, hi]);
            self.started = true;
            body = chunk.get(2..).unwrap_or(&[]);
        }
        for &b in body {
            let at = self.len as usize;
            if at >= self.total as usize {
                self.overflow = true;
                return;
            }
            match dst.get_mut(at) {
                Some(slot) => *slot = b,
                None => {
                    self.overflow = true;
                    return;
                }
            }
            self.len += 1;
        }
    }

    /// Whether the param was given at all.
    pub fn given(&self) -> bool {
        self.started
    }

    /// The blob's length once it arrived whole and fit; `None` for one that
    /// was cut short or ran past its buffer — a truncated program decodes to
    /// a different program, so it is never handed on.
    pub fn whole(&self) -> Option<usize> {
        (self.started && !self.overflow && self.len == self.total).then_some(self.len as usize)
    }
}
