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
// A read that answers "not yet" (`WouldBlock`) decides nothing: it is not "no
// checkpoint". The load is held where it was — the pointer already followed is
// kept in [`CkptLoad`], the pending read's handle in its [`ReadHold`] — and the
// caller asks again on a later step.
//
// Requires `blobstore_core` and a `SyscallTable`.

/// Store key holding the digest of the most recent checkpoint.
pub const KEY_LATEST: &[u8] = b"state/local/latest";

/// Why a checkpoint-store operation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CkptError {
    /// The underlying object store rejected the operation.
    Store,
    /// The store took a write but has not decided it yet.
    Pending,
    /// A read answered "not yet": nothing is decided. Hold the operation and
    /// ask again on a later step; never read this as "no checkpoint".
    WouldBlock,
    /// No checkpoint has been saved, or the one referenced is absent.
    NotFound,
    /// The store refused a read for a reason other than absence (its errno).
    /// Never "no checkpoint": the pointer or body may exist.
    ReadFailed(i32),
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
            BlobError::Pending => CkptError::Pending,
            BlobError::WouldBlock => CkptError::WouldBlock,
            BlobError::NotFound => CkptError::NotFound,
            BlobError::ReadFailed(e) => CkptError::ReadFailed(e),
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

/// A `latest` load in progress, kept across steps: the pointer once followed,
/// and the read parked on a "not yet". A load that ends (answer or error)
/// leaves it fresh for the next.
pub struct CkptLoad {
    pub hold: ReadHold,
    digest: [u8; 32],
    have_digest: bool,
}

impl CkptLoad {
    pub const fn new() -> Self {
        Self {
            hold: ReadHold::new(),
            digest: [0u8; 32],
            have_digest: false,
        }
    }
}

/// Read the checkpoint `latest` points at. Returns its length in `dst`.
///
/// `WouldBlock`: the load is held in `load` — the pointer, if already read,
/// is not read again — and the caller asks again, same `load`, on a later step.
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn ckpt_load_latest(
    sys: &SyscallTable,
    load: &mut CkptLoad,
    dst: &mut [u8],
) -> Result<(usize, [u8; 32]), CkptError> {
    let r = ckpt_load_latest_inner(sys, load, dst);
    if !matches!(r, Err(CkptError::WouldBlock)) {
        load.hold.release(sys);
        load.have_digest = false;
    }
    r
}

/// # Safety
/// `sys` must be the module's live syscall table.
unsafe fn ckpt_load_latest_inner(
    sys: &SyscallTable,
    load: &mut CkptLoad,
    dst: &mut [u8],
) -> Result<(usize, [u8; 32]), CkptError> {
    if !load.have_digest {
        let mut hex = [0u8; 64];
        let n = get_named(sys, &mut load.hold, KEY_LATEST, &mut hex)?;
        if n != 64 {
            return Err(CkptError::CorruptPointer);
        }
        load.digest = digest_of(&hex).ok_or(CkptError::CorruptPointer)?;
        load.have_digest = true;
    }
    let digest = load.digest;
    let len = blob_get(sys, &mut load.hold, &digest, dst)?; // verifies the content address
    Ok((len, digest))
}

/// Read a specific checkpoint by digest, verifying content addressing.
/// `WouldBlock`: ask again, same `hold`, on a later step.
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn ckpt_load(
    sys: &SyscallTable,
    hold: &mut ReadHold,
    digest: &[u8; 32],
    dst: &mut [u8],
) -> Result<usize, CkptError> {
    Ok(blob_get(sys, hold, digest, dst)?)
}

/// Whether a checkpoint has ever been saved.
///
/// `Err(WouldBlock)` is "not known yet" — neither yes nor no: ask again, same
/// `hold`, on a later step. Only an absent pointer is `Ok(false)`; a read the
/// store refused is `Err(ReadFailed)`, never "none".
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn ckpt_has_latest(sys: &SyscallTable, hold: &mut ReadHold) -> Result<bool, CkptError> {
    let mut hex = [0u8; 64];
    match get_named(sys, hold, KEY_LATEST, &mut hex) {
        Ok(n) => Ok(n == 64),
        Err(BlobError::NotFound) => Ok(false),
        Err(e) => Err(e.into()),
    }
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
