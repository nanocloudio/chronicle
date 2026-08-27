// Bounded, no_std, no-alloc RECORD-LIFECYCLE core — the one framed-admission and
// pending-delivery discipline every steady-state Chronicle module runs. `include!`d
// by the host crate (tests) and every streaming `.fmod`.
//
// WHY A SEAM. The lifecycle must be driven across `module_step` calls (peek, read,
// stage, retain), so proving "forced EAGAIN loses nothing" requires driving it in a
// host test. `Chan` is a four-method passthrough over the Fluxor channel syscalls
// with NO buffering or reordering semantics of its own — the production impl is a
// newtype over `&SyscallTable` (the only `unsafe`), the test impl is a scripted
// fake. This keeps the domain step logic pure safe-slice-in/typed-outcome-out;
// it is a mockable boundary, not a second channel abstraction.
//
// WHY PEEK. The typed record frame (`pipeline_core`) is self-delimiting: `frame_len`
// recovers a frame's total length from its own field-length chain. `channel_peek`
// copies head bytes WITHOUT advancing the read cursor, so a consumer can confirm a
// whole frame is present before it destructively reads exactly that frame. A frame
// still arriving is simply left unconsumed and retried next step — no partial
// acceptance, no retained-carry buffer for self-delimiting frames.

/// Poll event: input readable.
pub const POLL_IN: u32 = 0x01;
/// Poll event: output writable.
pub const POLL_OUT: u32 = 0x02;

/// A four-method passthrough over one Fluxor channel handle. Return conventions
/// mirror the syscalls exactly: `poll` returns a ready-event bitmask (`<=0` not
/// ready); `peek`/`read` return bytes copied (`0` empty, `<0` errno); `write`
/// returns bytes written (`0` the ring is full — retain and retry, `<0` errno).
pub trait Chan {
    fn poll(&self, events: u32) -> i32;
    /// Copy up to `buf.len()` head bytes without advancing the read cursor.
    fn peek(&self, buf: &mut [u8]) -> i32;
    /// Consume up to `buf.len()` bytes.
    fn read(&self, buf: &mut [u8]) -> i32;
    /// All-or-nothing write of `data`.
    fn write(&self, data: &[u8]) -> i32;
}

/// Map a non-positive `channel_write`/`channel_read` return to a reason.
#[inline]
pub fn classify_write(ret: i32) -> Reason {
    if ret == 0 {
        Reason::WouldBlock
    } else {
        // A negative channel return is an I/O-class dependency error; the errno is
        // not distinguished further.
        Reason::Io
    }
}

/// The outcome of trying to admit exactly one framed input unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admit {
    /// `buf[..len]` now holds exactly one consumed frame. Content validity is the
    /// domain decoder's job; the BOUNDARY is trusted because `frame_len` walked the
    /// frame's own length chain and found it wholly present and in-bounds.
    Complete(usize),
    /// No complete frame is present yet; NOTHING was consumed. Retry next step.
    NeedMore,
    /// The head of the channel does not resolve to a frame within one max_record
    /// buffer: the boundary cannot be trusted (a producer exceeded max_record, which
    /// compose-time admission is meant to prevent, or the stream is corrupt).
    /// NOTHING was consumed — a self-delimiting frame with no outer length cannot be
    /// safely skipped without desyncing the next one, so the caller must fault and
    /// reset the channel (`drain_all`), never continue reading.
    BoundaryLost,
    /// The channel held no readable bytes.
    Empty,
    /// A channel syscall failed.
    ChanError(Reason),
}

/// Admit at most one self-delimiting frame from `chan` into `buf`, non-destructively
/// confirming completeness before consuming. `frame_len(bytes) -> Option<total>` is
/// the format's boundary function (e.g. `pipeline_core::frame_len`); it returns
/// `Some(total)` only when a whole frame is present in `bytes` (`total <= bytes.len()`)
/// and `None` while the frame is still truncated. `buf` must be sized to the port's
/// max_record.
pub fn admit_frame(
    chan: &impl Chan,
    buf: &mut [u8],
    frame_len: fn(&[u8]) -> Option<usize>,
) -> Admit {
    let p = chan.poll(POLL_IN);
    if p <= 0 || (p as u32 & POLL_IN) == 0 {
        return Admit::Empty;
    }
    let n = chan.peek(buf);
    if n == 0 {
        return Admit::Empty;
    }
    if n < 0 {
        return Admit::ChanError(Reason::Io);
    }
    let n = n as usize;
    match frame_len(&buf[..n]) {
        // `frame_len` only returns `Some` when the whole frame is present, so
        // `fl <= n`. Consume EXACTLY the frame.
        Some(fl) => {
            let r = chan.read(&mut buf[..fl]);
            if r != fl as i32 {
                // peek promised `fl` bytes; a short read is a kernel-invariant break.
                return Admit::ChanError(Reason::Io);
            }
            Admit::Complete(fl)
        }
        // Header chain incomplete and room remains in the buffer: still arriving.
        None if n < buf.len() => Admit::NeedMore,
        // The buffer is full and still no frame resolved: the frame exceeds
        // max_record. Boundary lost; consume nothing and let the caller reset.
        None => Admit::BoundaryLost,
    }
}

/// Read and discard everything currently readable, in bounded chunks. The channel's
/// reset primitive after `BoundaryLost` or a terminal fault. Returns the number of
/// bytes drained.
pub fn drain_all(chan: &impl Chan, buf: &mut [u8]) -> usize {
    let mut total = 0usize;
    loop {
        let p = chan.poll(POLL_IN);
        if p <= 0 || (p as u32 & POLL_IN) == 0 {
            break;
        }
        let r = chan.read(buf);
        if r <= 0 {
            break;
        }
        total += r as usize;
    }
    total
}

/// The result of attempting to deliver a staged output frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Staged {
    /// Fully accepted by the downstream channel.
    Delivered,
    /// Retained for a later step; the module must not admit new input or overwrite
    /// the pending buffer until it drains.
    Pending,
    /// Abandoned after a terminal (non-`WouldBlock`) channel failure.
    Failed(Reason),
}

/// A single retained output frame. The frame bytes live in a caller-owned buffer
/// (`out_buf` in module state); this cursor tracks how much of it is still
/// undelivered. Mirrors the SDK `drain_pending`/`track_pending` helpers, but over
/// the testable `Chan` seam. At most one frame is retained at a time (a module
/// does not read new input while output is pending).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pending {
    pub off: u16,
    pub len: u16,
}

impl Pending {
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Flush retained bytes. Call at the top of `module_step`. `buf` must be the same
    /// buffer the frame was staged from.
    pub fn drain(&mut self, chan: &impl Chan, buf: &[u8]) -> Staged {
        if self.len == 0 {
            return Staged::Delivered;
        }
        let po = chan.poll(POLL_OUT);
        if po <= 0 || (po as u32 & POLL_OUT) == 0 {
            return Staged::Pending;
        }
        let start = self.off as usize;
        let end = start + self.len as usize;
        let w = chan.write(&buf[start..end]);
        if w > 0 {
            if (w as usize) >= self.len as usize {
                self.off = 0;
                self.len = 0;
                Staged::Delivered
            } else {
                self.off += w as u16;
                self.len -= w as u16;
                Staged::Pending
            }
        } else if w == 0 {
            Staged::Pending
        } else {
            self.off = 0;
            self.len = 0;
            Staged::Failed(classify_write(w))
        }
    }

    /// Attempt to deliver `buf[..len]` now; retain it on a full ring. The caller has
    /// already encoded the frame into `buf` and must not overwrite it while this
    /// returns `Pending`. Requires `self.is_empty()` (one frame at a time).
    pub fn stage(&mut self, chan: &impl Chan, buf: &[u8], len: usize) -> Staged {
        debug_assert!(self.is_empty(), "stage called with output already pending");
        if len == 0 {
            return Staged::Delivered;
        }
        let po = chan.poll(POLL_OUT);
        if po <= 0 || (po as u32 & POLL_OUT) == 0 {
            self.off = 0;
            self.len = len as u16;
            return Staged::Pending;
        }
        let w = chan.write(&buf[..len]);
        if w == len as i32 {
            Staged::Delivered
        } else if w > 0 {
            self.off = w as u16;
            self.len = (len - w as usize) as u16;
            Staged::Pending
        } else if w == 0 {
            self.off = 0;
            self.len = len as u16;
            Staged::Pending
        } else {
            Staged::Failed(classify_write(w))
        }
    }
}

/// The disposition of one steady-state module step, shared by every wrapper's
/// step core. The ABI shell maps it to the module's counters; a host harness
/// asserts it alongside the channel effects. `Idle`/`Pending` are non-terminal
/// (nothing to count); the rest are terminal for one input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepResult {
    /// No readable input and no pending output.
    Idle,
    /// A complete input was processed and its output fully delivered.
    Delivered,
    /// Output is retained; progress was made but the step must be re-driven.
    Pending,
    /// A deliberate zero-output policy filter — terminal and counted, never output
    /// pressure. Only modules with a drop policy (e.g. decision) return this.
    Dropped,
    /// A complete input was refused at admission (untrusted frame boundary).
    Rejected(Reason),
    /// An admitted input reached a terminal processing failure.
    Failed(Reason),
}
