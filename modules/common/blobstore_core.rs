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
// Requires `pb_core`-style buffer discipline (caller-provided, no alloc) plus
// the fluxor SDK `sha256` and a `SyscallTable`.

/// `storage.object` opcodes (fluxor contract vocabulary).
pub const OBJ_PUT: u32 = 0x1420;
pub const OBJ_GET: u32 = 0x1421;
pub const OBJ_RANGE_GET: u32 = 0x1423;
pub const OBJ_CLOSE: u32 = 0x1425;

/// Longest key this core builds: the prefix plus 64 hex characters.
pub const BLOB_KEY_MAX: usize = 96;

/// Why a blob operation failed. Values, never panics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobError {
    /// The provider rejected the write.
    PutFailed,
    /// No object at that digest.
    NotFound,
    /// The caller's buffer cannot hold the object.
    TooLarge,
    /// The bytes read do not hash to the digest they were stored under — the
    /// content-addressing invariant is broken, so the read fails rather than
    /// returning bytes that lie about their identity.
    DigestMismatch,
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
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn get_named(
    sys: &SyscallTable,
    key: &[u8],
    dst: &mut [u8],
) -> Result<usize, BlobError> {
    if key.len() > BLOB_KEY_MAX {
        return Err(BlobError::TooLarge);
    }
    let mut kbuf = [0u8; BLOB_KEY_MAX];
    kbuf[..key.len()].copy_from_slice(key);
    get_raw(sys, &mut kbuf, key.len(), dst)
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

    if (sys.provider_call)(-1, OBJ_PUT, arg.as_mut_ptr(), p) == 0 {
        Ok(())
    } else {
        Err(BlobError::PutFailed)
    }
}

/// Read the blob stored under `digest` into `dst`, VERIFYING the content
/// address. Returns the byte count.
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn blob_get(
    sys: &SyscallTable,
    digest: &[u8; 32],
    dst: &mut [u8],
) -> Result<usize, BlobError> {
    let mut key = [0u8; BLOB_KEY_MAX];
    let klen = blob_key(digest, &mut key);
    let n = get_raw(sys, &mut key, klen, dst)?;
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
/// once it has seen every byte.
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn blob_range(
    sys: &SyscallTable,
    digest: &[u8; 32],
    offset: u64,
    dst: &mut [u8],
) -> Result<usize, BlobError> {
    let mut key = [0u8; BLOB_KEY_MAX];
    let klen = blob_key(digest, &mut key);
    get_range(sys, &mut key, klen, offset, dst)
}

/// # Safety
/// `sys` must be the module's live syscall table.
unsafe fn get_raw(
    sys: &SyscallTable,
    key: &mut [u8; BLOB_KEY_MAX],
    klen: usize,
    dst: &mut [u8],
) -> Result<usize, BlobError> {
    get_range(sys, key, klen, 0, dst)
}

/// # Safety
/// `sys` must be the module's live syscall table.
unsafe fn get_range(
    sys: &SyscallTable,
    key: &mut [u8; BLOB_KEY_MAX],
    klen: usize,
    offset: u64,
    dst: &mut [u8],
) -> Result<usize, BlobError> {
    let h = (sys.provider_call)(-1, OBJ_GET, key.as_mut_ptr(), klen);
    if h < 0 {
        return Err(BlobError::NotFound);
    }
    // RANGE_GET argument: [offset:u64][cap:u32][dst_ptr:u64]
    let mut rarg = [0u8; 20];
    rarg[0..8].copy_from_slice(&offset.to_le_bytes());
    rarg[8..12].copy_from_slice(&(dst.len() as u32).to_le_bytes());
    rarg[12..20].copy_from_slice(&(dst.as_mut_ptr() as u64).to_le_bytes());
    let n = (sys.provider_call)(h, OBJ_RANGE_GET, rarg.as_mut_ptr(), 20);
    let mut carg = [0u8; 4];
    (sys.provider_call)(h, OBJ_CLOSE, carg.as_mut_ptr(), 0);

    if n < 0 {
        return Err(BlobError::NotFound);
    }
    let n = n as usize;
    if n > dst.len() {
        return Err(BlobError::TooLarge);
    }
    Ok(n)
}
