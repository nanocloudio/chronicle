// Release manifests on device — deciding WHICH versions a fleet runs, not just
// running them.
//
// `version_core` already reads a version table, applies hot-reload control
// messages, and builds a `versions` param. This is the control-plane half: the
// validation that a manifest is representable before it is committed, the
// control messages that converge a running instance onto it, and the reconciler
// that diffs an observed release against what an instance has applied.
//
// A version is identified by a CONTENT DIGEST, so the same tag resolves to the
// same bytecode on every instance. That is what makes a mixed-version fleet
// consistent: an instance behind the current revision fails closed for a version
// it has not loaded (the caller retries elsewhere) rather than serving different
// logic under the same tag. Validation exists to keep that promise — a manifest
// that cannot be represented on device must be refused at commit time, not
// discovered as a truncated param after rollout.
//
// Digests are passed IN rather than computed here: hashing is the SDK's job, and
// a core that reached for crypto would need unsafe code it has no other use for.
//
// Requires `version_core` (the entry layout, `vctl` opcodes and the bounds) and
// `barrier_core` (`ActivationBarrier`, which orders the default flip by
// replicated-log index).

/// Why a manifest was refused, or a message would not fit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseError {
    /// More versions than the device's table can hold.
    TooManyVersions,
    /// A tag longer than the entry layout's one-byte length allows.
    TagTooLong,
    /// A program longer than the entry layout's two-byte length allows.
    ProgramTooLarge,
    /// Two versions share a tag — the selector would be ambiguous.
    DuplicateTag,
    /// The default tag names no version in the manifest.
    UnknownDefaultTag,
    /// A digest of the wrong width for the entry layout.
    BadDigestLen,
    /// The output buffer cannot hold the message.
    TooLarge,
}

/// One version: a human tag, the program it runs, and that program's content
/// digest (`VERSION_DIGEST_LEN` bytes, computed by the caller).
#[derive(Clone, Copy)]
pub struct VersionRef<'a> {
    pub tag: &'a [u8],
    pub program: &'a [u8],
    pub digest: &'a [u8],
}

/// A module's active release: its versions plus which tag unselected traffic gets.
#[derive(Clone, Copy)]
pub struct ManifestRef<'a> {
    pub versions: &'a [VersionRef<'a>],
    pub default_tag: &'a [u8],
}

impl ManifestRef<'_> {
    /// Check the manifest is representable on device: bounded version count, tag
    /// and program lengths, well-formed digests, unique tags, and a resolvable
    /// default.
    ///
    /// Called before committing, so a manifest that would produce a truncated or
    /// ambiguous param is refused while it is still a deployment error rather
    /// than a fleet serving the wrong version.
    pub fn validate(&self) -> Result<(), ReleaseError> {
        if self.versions.len() > MAX_VERSIONS {
            return Err(ReleaseError::TooManyVersions);
        }
        for (i, v) in self.versions.iter().enumerate() {
            if v.tag.len() > VERSION_TAG_CAP {
                return Err(ReleaseError::TagTooLong);
            }
            if v.program.len() > u16::MAX as usize {
                return Err(ReleaseError::ProgramTooLarge);
            }
            if v.digest.len() != VERSION_DIGEST_LEN {
                return Err(ReleaseError::BadDigestLen);
            }
            // Quadratic, deliberately: MAX_VERSIONS is small and fixed, and a
            // set would need an allocator to save nothing measurable.
            for w in &self.versions[..i] {
                if w.tag == v.tag {
                    return Err(ReleaseError::DuplicateTag);
                }
            }
        }
        if self.index_of(self.default_tag).is_none() {
            return Err(ReleaseError::UnknownDefaultTag);
        }
        Ok(())
    }

    /// The index of the version tagged `tag`, if present.
    pub fn index_of(&self, tag: &[u8]) -> Option<usize> {
        let mut i = 0;
        while i < self.versions.len() {
            if self.versions[i].tag == tag {
                return Some(i);
            }
            i += 1;
        }
        None
    }

    /// The content digest of the version tagged `tag`, if present.
    pub fn digest_of(&self, tag: &[u8]) -> Option<&[u8]> {
        self.index_of(tag).map(|i| self.versions[i].digest)
    }

    /// Whether the manifest carries a version under `tag`.
    pub fn has_tag(&self, tag: &[u8]) -> bool {
        self.index_of(tag).is_some()
    }
}

/// `[op][digest:8][tag_len:u8][tag][prog_len:u16 LE][prog]` — the two add forms
/// share an entry layout and differ only in opcode and payload meaning.
fn add_msg(
    out: &mut [u8],
    op: u8,
    tag: &[u8],
    payload: &[u8],
    digest: &[u8],
) -> Result<usize, ReleaseError> {
    if digest.len() != VERSION_DIGEST_LEN {
        return Err(ReleaseError::BadDigestLen);
    }
    if out.is_empty() {
        return Err(ReleaseError::TooLarge);
    }
    out[0] = op;
    // Through version_core's single entry writer, so the host, the hot-reload
    // path and this never encode the layout three different ways.
    write_version_entry(out, 1, digest, tag, payload).ok_or(ReleaseError::TooLarge)
}

/// `[op][tag_len:u8][tag]` — the two tag-only control messages.
fn tag_msg(out: &mut [u8], op: u8, tag: &[u8]) -> Result<usize, ReleaseError> {
    if tag.len() > 255 {
        return Err(ReleaseError::TagTooLong);
    }
    if out.len() < 2 + tag.len() {
        return Err(ReleaseError::TooLarge);
    }
    out[0] = op;
    out[1] = tag.len() as u8;
    out[2..2 + tag.len()].copy_from_slice(tag);
    Ok(2 + tag.len())
}

/// Hot-reload control message: add (or replace, if the tag exists) a version
/// carrying pre-lowered bytecode.
pub fn add_version_msg(
    out: &mut [u8],
    tag: &[u8],
    program: &[u8],
    digest: &[u8],
) -> Result<usize, ReleaseError> {
    add_msg(out, vctl::ADD_VERSION, tag, program, digest)
}

/// Hot-reload control message: add (or replace) a version carrying an IR-stages
/// container the target LOWERS on apply.
///
/// Ships the checked IR to a running instance so it re-derives the bytecode
/// itself — the same self-validation the `ir_stages` param gets at load, now for
/// hot reload. The digest is the identity of the shipped IR, not of the bytecode
/// it lowers to.
pub fn add_version_ir_msg(
    out: &mut [u8],
    tag: &[u8],
    ir_stages: &[u8],
    digest: &[u8],
) -> Result<usize, ReleaseError> {
    add_msg(out, vctl::ADD_VERSION_IR, tag, ir_stages, digest)
}

/// Hot-reload control message: repoint the default to `tag` — the blue-green flip.
pub fn set_default_msg(out: &mut [u8], tag: &[u8]) -> Result<usize, ReleaseError> {
    tag_msg(out, vctl::SET_DEFAULT, tag)
}

/// Hot-reload control message: remove a drained version, reclaiming its slot.
///
/// Reconciliation never emits this. A `Reconciler` only adds and flips; removing
/// a version is an explicit operator action once it has drained, because nothing
/// in a manifest diff can tell you the old version has no traffic left.
pub fn remove_version_msg(out: &mut [u8], tag: &[u8]) -> Result<usize, ReleaseError> {
    tag_msg(out, vctl::REMOVE_VERSION, tag)
}

/// What a reconcile step decided, without the messages themselves.
///
/// The messages are written into the caller's buffer as they are produced (there
/// is nowhere to accumulate them without an allocator), so this reports the
/// SHAPE of the convergence: how many messages, how many were adds, and whether
/// the default moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconcilePlan {
    pub revision: u64,
    pub n_messages: usize,
    pub n_added: usize,
    pub default_flipped: bool,
}

/// One instance's view of the release.
///
/// Holds only the applied revision, the applied default tag and the barrier —
/// not the applied manifest. An instance does not need to remember every version
/// it has loaded, because the module's own version table already is that record;
/// `reconcile` is told what is loaded rather than keeping a second copy that
/// could disagree with the first.
pub struct Reconciler {
    applied_revision: u64,
    default_tag: [u8; VERSION_TAG_CAP],
    default_tag_len: u8,
    /// False until the first successful reconcile, so revision 0 is distinguishable
    /// from "no default yet" without a sentinel tag.
    has_default: bool,
    barrier: ActivationBarrier,
}

impl Default for Reconciler {
    fn default() -> Self {
        Self::new()
    }
}

impl Reconciler {
    pub fn new() -> Self {
        Reconciler {
            applied_revision: 0,
            default_tag: [0u8; VERSION_TAG_CAP],
            default_tag_len: 0,
            has_default: false,
            barrier: ActivationBarrier::new(),
        }
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied_revision
    }

    /// The default tag this instance currently serves, if it has reconciled.
    pub fn default_tag(&self) -> Option<&[u8]> {
        if self.has_default {
            Some(&self.default_tag[..self.default_tag_len as usize])
        } else {
            None
        }
    }

    /// The digest routing currently resolves the default to (via the barrier).
    ///
    /// A full 32-byte sha256: the barrier carries the whole digest even though
    /// the version TABLE keys on a shorter prefix, so a rollback target is never
    /// ambiguous.
    pub fn active_default_digest(&self) -> Option<[u8; 32]> {
        self.barrier.active()
    }

    /// The barrier itself, for staging and rollback outside a reconcile step.
    pub fn barrier(&mut self) -> &mut ActivationBarrier {
        &mut self.barrier
    }

    /// Diff `manifest` at `revision` against what this instance has applied and
    /// write the control messages that converge it.
    ///
    /// `loaded` answers "does this instance already have this tag?" — normally
    /// backed by the module's own version table. Messages are written to
    /// `out[i]` in the order they must be applied: every add first, then the
    /// default flip, so the default never points at a version not yet present.
    ///
    /// Returns `None` when `revision` is not newer than what is applied, which
    /// makes repeated observation of the same release a no-op rather than a
    /// stream of redundant reloads.
    pub fn reconcile(
        &mut self,
        revision: u64,
        manifest: &ManifestRef,
        loaded: &dyn Fn(&[u8]) -> bool,
        out: &mut [&mut [u8]],
        lens: &mut [usize],
    ) -> Result<Option<ReconcilePlan>, ReleaseError> {
        if revision <= self.applied_revision && self.has_default {
            return Ok(None);
        }
        manifest.validate()?;

        let mut n = 0usize;
        let mut n_added = 0usize;
        for v in manifest.versions {
            if loaded(v.tag) {
                continue;
            }
            if n >= out.len() || n >= lens.len() {
                return Err(ReleaseError::TooLarge);
            }
            lens[n] = add_version_msg(out[n], v.tag, v.program, v.digest)?;
            n += 1;
            n_added += 1;
        }

        let flipped = self.default_tag() != Some(manifest.default_tag);
        if flipped {
            if n >= out.len() || n >= lens.len() {
                return Err(ReleaseError::TooLarge);
            }
            lens[n] = set_default_msg(out[n], manifest.default_tag)?;
            n += 1;
        }

        // Stage and commit at the revision: the barrier orders the flip by the
        // replicated-log index, so coexistence and rollback stay index-ordered
        // rather than arrival-ordered.
        if let Some(dg) = manifest.digest_of(manifest.default_tag) {
            let mut d = [0u8; 32];
            let w = if dg.len() > 32 { 32 } else { dg.len() };
            d[..w].copy_from_slice(&dg[..w]);
            self.barrier.stage(revision, d);
            self.barrier.commit(revision);
        }

        // `validate` bounded the tag at VERSION_TAG_CAP, so this cannot truncate.
        let t = manifest.default_tag;
        self.default_tag[..t.len()].copy_from_slice(t);
        self.default_tag_len = t.len() as u8;
        self.has_default = true;
        self.applied_revision = revision;

        Ok(Some(ReconcilePlan {
            revision,
            n_messages: n,
            n_added,
            default_flipped: flipped,
        }))
    }
}
