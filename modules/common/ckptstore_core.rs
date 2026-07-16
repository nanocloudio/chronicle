// The durable local checkpoint backend (`state.local.checkpoint.v1`), on device.
//
// `durable-state-e2e.sh` already showed a checkpoint surviving a restart, but it
// did so by having the operator carry the digest from one run to the next. That
// proves the state transition; it does not give a node the ability to RECOVER BY
// ITSELF, which is what the capability actually promises. A node coming back up
// after a crash has no one to tell it which checkpoint was the last good one.
//
// So the contract is content-addressed storage PLUS a mutable `latest` pointer:
//
//   save        -> store the snapshot under its own sha256, then point `latest`
//                  at that digest
//   load_latest -> follow the pointer and read the snapshot back, VERIFIED
//   load        -> read a specific checkpoint by digest, VERIFIED
//
// The split mirrors the OCI store for the same reason: content is immutable and
// self-verifying, pointers are mutable and are not. `latest` deliberately holds
// a digest rather than the bytes — a pointer that could disagree with its target
// would reintroduce exactly the ambiguity content addressing removes.
//
// Checkpoints share the content-addressed key space with every other blob, so
// two identical checkpoints dedupe to one object rather than accumulating.
//
// ORDER MATTERS in `save`: the body is stored BEFORE the pointer moves. A crash
// between the two leaves `latest` pointing at the previous checkpoint — older
// state, but consistent. The reverse order would leave it pointing at bytes that
// were never written, which is unrecoverable.
//
// Requires `blobstore_core` and a `SyscallTable`.

/// Store key holding the digest of the most recent checkpoint.
pub const KEY_LATEST: &[u8] = b"state/local/latest";

/// Why a checkpoint-store operation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CkptError {
    /// The underlying object store rejected the operation.
    Store,
    /// No checkpoint has been saved, or the one referenced is absent.
    NotFound,
    /// The caller's buffer cannot hold the checkpoint.
    TooLarge,
    /// The bytes read do not hash to the digest they were stored under.
    DigestMismatch,
    /// The `latest` pointer does not contain a 64-character hex digest.
    CorruptPointer,
}

impl From<BlobError> for CkptError {
    fn from(e: BlobError) -> Self {
        match e {
            BlobError::PutFailed => CkptError::Store,
            BlobError::NotFound => CkptError::NotFound,
            BlobError::TooLarge => CkptError::TooLarge,
            BlobError::DigestMismatch => CkptError::DigestMismatch,
        }
    }
}

/// Persist `snapshot` and move `latest` to it. Returns the digest.
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn ckpt_save(sys: &SyscallTable, snapshot: &[u8]) -> Result<[u8; 32], CkptError> {
    // Body first: a crash before the pointer moves loses this checkpoint but
    // leaves the previous one intact and resolvable.
    let digest = blob_put(sys, snapshot)?;
    let mut hex = [0u8; 64];
    hex_of(&digest, &mut hex);
    put_named(sys, KEY_LATEST, &hex)?;
    Ok(digest)
}

/// Read the checkpoint `latest` points at. Returns its length in `dst`.
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn ckpt_load_latest(
    sys: &SyscallTable,
    dst: &mut [u8],
) -> Result<(usize, [u8; 32]), CkptError> {
    let mut hex = [0u8; 64];
    let n = get_named(sys, KEY_LATEST, &mut hex)?;
    if n != 64 {
        return Err(CkptError::CorruptPointer);
    }
    let digest = digest_of(&hex).ok_or(CkptError::CorruptPointer)?;
    let len = blob_get(sys, &digest, dst)?; // verifies the content address
    Ok((len, digest))
}

/// Read a specific checkpoint by digest, verifying content addressing.
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn ckpt_load(
    sys: &SyscallTable,
    digest: &[u8; 32],
    dst: &mut [u8],
) -> Result<usize, CkptError> {
    Ok(blob_get(sys, digest, dst)?)
}

/// Whether a checkpoint has ever been saved.
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn ckpt_has_latest(sys: &SyscallTable) -> bool {
    let mut hex = [0u8; 64];
    get_named(sys, KEY_LATEST, &mut hex) == Ok(64)
}

fn hex_of(digest: &[u8; 32], out: &mut [u8; 64]) {
    const HEXD: &[u8; 16] = b"0123456789abcdef";
    let mut n = 0;
    for b in digest {
        out[n] = HEXD[(b >> 4) as usize];
        out[n + 1] = HEXD[(b & 0x0f) as usize];
        n += 2;
    }
}

fn digest_of(hex: &[u8; 64]) -> Option<[u8; 32]> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }
    let mut out = [0u8; 32];
    let mut i = 0;
    while i < 32 {
        out[i] = (val(hex[i * 2])? << 4) | val(hex[i * 2 + 1])?;
        i += 1;
    }
    Some(out)
}
