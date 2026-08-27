// The production `io_core::Chan` implementation over one Fluxor channel handle.
// This is the ONLY place io_core's testable seam meets the raw syscalls; it is
// mounted by every streaming module AFTER `abi` (for `SyscallTable`) and `io_core`
// (for `Chan`/`POLL_*`). Host tests substitute a scripted fake, so this file needs
// no host coverage — the `.fmod` E2E gates exercise it against the real runtime.
//
// A four-line pass-through: no buffering, no retry, no reordering. Every lifecycle
// decision lives in io_core against these primitives; confining the `unsafe` here
// keeps the domain logic safe and mockable.

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
        unsafe { (self.sys.channel_write)(self.handle, data.as_ptr(), data.len()) }
    }
}
