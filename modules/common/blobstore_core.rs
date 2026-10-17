// Content-addressed blob storage over fluxor's `storage.object` contract.
//
// A node that can SEAL an artefact (`artefact_core`) also has to KEEP and SERVE
// it, or the identity it computed goes nowhere. This stores a blob under its own
// sha256 — the addressing an OCI layout uses for `blobs/sha256/<hex>` — so a
// consumer resolves a pin to exactly the bytes that were sealed.
//
// Content addressing is only a guarantee if it is CHECKED. `blob_get` rehashes
// what it read and refuses a mismatch: a truncated read, a partial write, or a
// tampered store would otherwise return plausible bytes under a digest that no
// longer describes them.
//
// A write is single-shot. A provider may answer `EINPROGRESS` — taken, not yet
// decided — and this core reports that as `Pending` rather than as a failure:
// the write may still land. Repeating a blob put is safe, because its key is the
// hash of its body: it can only write the same object again. A store that
// decides inside the call never answers pending.
//
// A read can be "not yet" too: `GET` and `RANGE_GET` MAY answer `EAGAIN` while
// the provider's backing fetch has not landed. That is never absence: the read
// reports `WouldBlock`, decides nothing, and the caller holds its operation and
// asks the same read again on a LATER step (never within this one — a store
// frees room only between steps). The handle a `GET` opened is part of that
// held request: when `RANGE_GET` answers `EAGAIN` nothing was copied and the
// handle stays valid, so it is parked in the caller's [`ReadHold`] and the same
// `RANGE_GET` is asked on it next time, rather than abandoning the fetch already
// in flight with a second `GET`. A `GET` that answers `EAGAIN` opened nothing,
// so there is nothing to hold and the `GET` itself is asked again.
//
// Only the errnos the object contract gives absence — `ENOENT` (what a `GET`
// answers for a key with no object) and `ENXIO` (`HEAD`'s) — are `NotFound`.
// Any other refusal (`EIO`, `ENOSPC`, `EACCES`, …) is `ReadFailed`: the object
// may well be there, so a caller refuses, retries or reports it, and never
// takes it for "not there" — which would let an initializer write a fresh,
// empty object over real data.
//
// Requires `pb_core`-style buffer discipline (caller-provided, no alloc), the
// fluxor SDK `sha256`, and the SDK `abi` mounted at the crate root (for
// `SyscallTable` and the `storage.object` write answer).

/// `storage.object` opcodes (fluxor contract vocabulary).
pub const OBJ_PUT: u32 = 0x1420;
pub const OBJ_GET: u32 = 0x1421;
pub const OBJ_RANGE_GET: u32 = 0x1423;
pub const OBJ_CLOSE: u32 = 0x1425;

use crate::abi::contracts::storage::object::{write_answer, WriteAnswer};
use crate::abi::kernel_abi::errno::{EAGAIN, ENOENT, ENXIO};

/// Longest key this core builds: the prefix plus 64 hex characters.
pub const BLOB_KEY_MAX: usize = 96;

/// Why a blob operation failed. Values, never panics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobError {
    /// The provider rejected the write.
    PutFailed,
    /// The provider took the write but has not decided it (`EINPROGRESS`).
    /// Not a failure: the write may still land, and repeating it is safe.
    Pending,
    /// A read answered "not yet" (`EAGAIN`): nothing is decided — not
    /// absence, not failure. The caller holds its operation unchanged, issues
    /// no write after it, and asks the same read again on a later step.
    WouldBlock,
    /// No object at that key: the store answered `ENOENT` or `ENXIO`.
    NotFound,
    /// The store refused the read for a reason other than absence (`EIO`,
    /// `ENOSPC`, `EACCES`, …), carried as its errno. Not absence: the object
    /// may exist, so nothing may be written on the strength of this — refuse,
    /// retry or report it.
    ReadFailed(i32),
    /// The caller's buffer cannot hold the object.
    TooLarge,
    /// The bytes read do not hash to the digest they were stored under — the
    /// content-addressing invariant is broken, so the read fails rather than
    /// returning bytes that lie about their identity.
    DigestMismatch,
}

/// What a refused read (`rc < 0`, not `EAGAIN`) means: `NotFound` only for the
/// errnos the object contract gives absence, `ReadFailed` for every other.
pub fn read_refusal(rc: i32) -> BlobError {
    if rc == ENOENT || rc == ENXIO {
        BlobError::NotFound
    } else {
        BlobError::ReadFailed(rc)
    }
}

/// Write `blobs/sha256/<hex>` into `out`, returning its length. The layout an
/// OCI image directory uses, so a store written here is one a registry can serve.
pub fn blob_key(digest: &[u8; 32], out: &mut [u8; BLOB_KEY_MAX]) -> usize {
    const PREFIX: &[u8] = b"blobs/sha256/";
    out[..PREFIX.len()].copy_from_slice(PREFIX);
    let mut n = PREFIX.len();
    for b in digest {
        out[n] = HEXD[(b >> 4) as usize];
        out[n + 1] = HEXD[(b & 0x0f) as usize];
        n += 2;
    }
    n
}

const HEXD: &[u8; 16] = b"0123456789abcdef";

/// A read parked on a "not yet": the handle a `GET` opened, and the request
/// (`key`, `offset`, `cap`) whose `RANGE_GET` answered `EAGAIN`.
///
/// Caller-owned and kept across steps (module state), because the request it
/// holds outlives the call that made it. Asking the SAME read again resumes on
/// the held handle; a read that completes, fails or is superseded closes it. A
/// caller that abandons its operation calls [`ReadHold::release`].
pub struct ReadHold {
    handle: i32,
    key: [u8; BLOB_KEY_MAX],
    klen: usize,
    offset: u64,
    cap: usize,
}

impl ReadHold {
    /// Nothing held.
    pub const fn new() -> Self {
        Self {
            handle: -1,
            key: [0u8; BLOB_KEY_MAX],
            klen: 0,
            offset: 0,
            cap: 0,
        }
    }

    /// Whether a parked request holds a handle.
    pub fn is_held(&self) -> bool {
        self.handle >= 0
    }

    /// Whether the held request is exactly this one. `key` is zero-padded past
    /// `klen` on both sides, so whole arrays compare and nothing is sliced.
    fn holds(&self, key: &[u8; BLOB_KEY_MAX], klen: usize, offset: u64, cap: usize) -> bool {
        self.handle >= 0
            && self.klen == klen
            && self.offset == offset
            && self.cap == cap
            && self.key == *key
    }

    /// Close the held handle, if any, and forget the request.
    ///
    /// # Safety
    /// `sys` must be the module's live syscall table.
    pub unsafe fn release(&mut self, sys: &SyscallTable) {
        if self.handle >= 0 {
            let mut carg = [0u8; 4];
            (sys.provider_call)(self.handle, OBJ_CLOSE, carg.as_mut_ptr(), 0);
        }
        *self = Self::new();
    }
}

/// Store `body` under an arbitrary `key`.
///
/// The mutable, NAMED half of the store — `oci-layout` and `index.json` are
/// pointers, not content, so they cannot live under a content address. Blob
/// writes go through [`blob_put`], which is this with the key derived from the
/// body's own hash.
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn put_named(sys: &SyscallTable, key: &[u8], body: &[u8]) -> Result<(), BlobError> {
    if key.len() > BLOB_KEY_MAX {
        return Err(BlobError::TooLarge);
    }
    let mut kbuf = [0u8; BLOB_KEY_MAX];
    kbuf[..key.len()].copy_from_slice(key);
    put_raw(sys, &kbuf, key.len(), body)
}

/// Read the object stored under an arbitrary `key` into `dst`, returning the
/// byte count. No content-address check: a named object is a pointer, and its
/// bytes are whatever was last written there. Use [`blob_get`] for content.
///
/// `WouldBlock` is "not yet", never absence: ask the same read again, with the
/// same `hold`, on a later step.
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn get_named(
    sys: &SyscallTable,
    hold: &mut ReadHold,
    key: &[u8],
    dst: &mut [u8],
) -> Result<usize, BlobError> {
    if key.len() > BLOB_KEY_MAX {
        return Err(BlobError::TooLarge);
    }
    let mut kbuf = [0u8; BLOB_KEY_MAX];
    kbuf[..key.len()].copy_from_slice(key);
    get_raw(sys, hold, &mut kbuf, key.len(), dst)
}

/// Store `body` under its own sha256 and return that digest.
///
/// Idempotent by construction: the same bytes always land on the same key, so a
/// re-publish overwrites with identical content rather than duplicating.
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn blob_put(sys: &SyscallTable, body: &[u8]) -> Result<[u8; 32], BlobError> {
    let digest = sha256(body);
    let mut key = [0u8; BLOB_KEY_MAX];
    let klen = blob_key(&digest, &mut key);
    put_raw(sys, &key, klen, body)?;
    Ok(digest)
}

/// # Safety
/// `sys` must be the module's live syscall table.
unsafe fn put_raw(
    sys: &SyscallTable,
    key: &[u8; BLOB_KEY_MAX],
    klen: usize,
    body: &[u8],
) -> Result<(), BlobError> {
    // storage.object PUT argument layout:
    //   [key_len:u16][key][content_type_len:u8]
    //   [body_ptr:u64][body_len:u64][precondition:u8][etag_len:u8]
    //   [fence_ptr:u64][fence_cap:u16]
    let mut fence = [0u8; 62];
    let mut arg = [0u8; BLOB_KEY_MAX + 64];
    let mut p = 0usize;
    arg[p..p + 2].copy_from_slice(&(klen as u16).to_le_bytes());
    p += 2;
    arg[p..p + klen].copy_from_slice(&key[..klen]);
    p += klen;
    arg[p] = 0; // no content type
    p += 1;
    arg[p..p + 8].copy_from_slice(&(body.as_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 8].copy_from_slice(&(body.len() as u64).to_le_bytes());
    p += 8;
    arg[p] = 0; // precondition = ANY
    arg[p + 1] = 0; // etag_len
    p += 2;
    arg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 2].copy_from_slice(&62u16.to_le_bytes());
    p += 2;

    match write_answer((sys.provider_call)(-1, OBJ_PUT, arg.as_mut_ptr(), p)) {
        WriteAnswer::Decided(0) => Ok(()),
        WriteAnswer::Decided(_) => Err(BlobError::PutFailed),
        WriteAnswer::Pending => Err(BlobError::Pending),
    }
}

/// Read the blob stored under `digest` into `dst`, VERIFYING the content
/// address. Returns the byte count. `WouldBlock`: ask again, same `hold`,
/// on a later step.
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn blob_get(
    sys: &SyscallTable,
    hold: &mut ReadHold,
    digest: &[u8; 32],
    dst: &mut [u8],
) -> Result<usize, BlobError> {
    let mut key = [0u8; BLOB_KEY_MAX];
    let klen = blob_key(digest, &mut key);
    let n = get_raw(sys, hold, &mut key, klen, dst)?;
    // The invariant, actually checked.
    if sha256(&dst[..n]) != *digest {
        return Err(BlobError::DigestMismatch);
    }
    Ok(n)
}

/// Read a WINDOW of a stored object, starting at `offset`.
///
/// The whole-object [`blob_get`] cannot serve an artefact larger than a module
/// buffer — an OTA slot image is 512 KB — so anything that big is consumed a
/// window at a time. No digest check happens here, and cannot: a window is not
/// the content, so verifying the content address is the streaming caller's job
/// once it has seen every byte. `WouldBlock`: ask the same window again,
/// same `hold`, on a later step.
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn blob_range(
    sys: &SyscallTable,
    hold: &mut ReadHold,
    digest: &[u8; 32],
    offset: u64,
    dst: &mut [u8],
) -> Result<usize, BlobError> {
    let mut key = [0u8; BLOB_KEY_MAX];
    let klen = blob_key(digest, &mut key);
    get_range(sys, hold, &mut key, klen, offset, dst)
}

/// # Safety
/// `sys` must be the module's live syscall table.
unsafe fn get_raw(
    sys: &SyscallTable,
    hold: &mut ReadHold,
    key: &mut [u8; BLOB_KEY_MAX],
    klen: usize,
    dst: &mut [u8],
) -> Result<usize, BlobError> {
    get_range(sys, hold, key, klen, 0, dst)
}

/// One window read: `GET` (or the handle `hold` parked for this very
/// request), `RANGE_GET`, `CLOSE`.
///
/// `GET` answering `EAGAIN` opened nothing: `WouldBlock`, nothing held.
/// `RANGE_GET` answering `EAGAIN` copied nothing and left the handle valid:
/// `WouldBlock`, the handle parked in `hold` for the same request to resume
/// on. Any other answer closes the handle; a refusal is `NotFound` or
/// `ReadFailed` as [`read_refusal`] decides.
///
/// # Safety
/// `sys` must be the module's live syscall table.
unsafe fn get_range(
    sys: &SyscallTable,
    hold: &mut ReadHold,
    key: &mut [u8; BLOB_KEY_MAX],
    klen: usize,
    offset: u64,
    dst: &mut [u8],
) -> Result<usize, BlobError> {
    let cap = dst.len();
    let h = if hold.holds(key, klen, offset, cap) {
        hold.handle
    } else {
        // A different request supersedes whatever was parked.
        hold.release(sys);
        let h = (sys.provider_call)(-1, OBJ_GET, key.as_mut_ptr(), klen);
        if h == EAGAIN {
            return Err(BlobError::WouldBlock);
        }
        if h < 0 {
            return Err(read_refusal(h));
        }
        h
    };
    // RANGE_GET argument: [offset:u64][cap:u32][dst_ptr:u64]
    let mut rarg = [0u8; 20];
    rarg[0..8].copy_from_slice(&offset.to_le_bytes());
    rarg[8..12].copy_from_slice(&(cap as u32).to_le_bytes());
    rarg[12..20].copy_from_slice(&(dst.as_mut_ptr() as u64).to_le_bytes());
    let n = (sys.provider_call)(h, OBJ_RANGE_GET, rarg.as_mut_ptr(), 20);
    if n == EAGAIN {
        // Nothing copied; the handle stays valid and is the request's.
        hold.handle = h;
        hold.key = *key;
        hold.klen = klen;
        hold.offset = offset;
        hold.cap = cap;
        return Err(BlobError::WouldBlock);
    }
    let mut carg = [0u8; 4];
    (sys.provider_call)(h, OBJ_CLOSE, carg.as_mut_ptr(), 0);
    *hold = ReadHold::new();

    if n < 0 {
        return Err(read_refusal(n));
    }
    let n = n as usize;
    if n > dst.len() {
        return Err(BlobError::TooLarge);
    }
    Ok(n)
}
