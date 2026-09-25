// The production `io_core::Chan` implementation over one Fluxor channel handle.
// This is the ONLY place io_core's testable seam meets the raw syscalls; it is
// mounted by every streaming module AFTER `abi` (for `SyscallTable`) and `io_core`
// (for `Chan`/`POLL_*`). Host tests substitute a scripted fake, so this file needs
// no host coverage — the `.fmod` E2E gates exercise it against the real runtime.
//
// A pass-through: no buffering, no retry, no reordering. Every lifecycle decision
// lives in io_core against these primitives; confining the `unsafe` here keeps the
// domain logic safe and mockable.
//
// ONE translation: the kernel answers a write to a full ring with `EAGAIN`, where
// `Chan::write` promises `0` ("retain and retry"). A FIFO write is all-or-nothing,
// so `POLL_OUT` (some space) followed by a frame larger than that space is exactly
// this case, and it is transient: the frame must be retained and retried, never
// counted as a failure that drops it. A frame larger than the whole ring would
// retain forever; the loader refuses any wiring whose declared `max_record`
// exceeds the ring, and every module mounting this seam declares one.

/// The kernel's "ring full, nothing written" answer to `channel_write` (`EAGAIN`).
const WRITE_FULL: i32 = -11;

/// A borrowed `(syscall table, handle)` pair presented as an `io_core::Chan`.
pub struct SysChan<'a> {
    sys: &'a SyscallTable,
    handle: i32,
}

impl<'a> SysChan<'a> {
    #[inline]
    pub fn new(sys: &'a SyscallTable, handle: i32) -> Self {
        Self { sys, handle }
    }
}

impl Chan for SysChan<'_> {
    #[inline]
    fn poll(&self, events: u32) -> i32 {
        unsafe { (self.sys.channel_poll)(self.handle, events) }
    }
    #[inline]
    fn peek(&self, buf: &mut [u8]) -> i32 {
        unsafe { (self.sys.channel_peek)(self.handle, buf.as_mut_ptr(), buf.len()) }
    }
    #[inline]
    fn read(&self, buf: &mut [u8]) -> i32 {
        unsafe { (self.sys.channel_read)(self.handle, buf.as_mut_ptr(), buf.len()) }
    }
    #[inline]
    fn write(&self, data: &[u8]) -> i32 {
        let r = unsafe { (self.sys.channel_write)(self.handle, data.as_ptr(), data.len()) };
        if r == WRITE_FULL {
            0
        } else {
            r
        }
    }
}
