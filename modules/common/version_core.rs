// Bounded, no_std, no-alloc MULTI-VERSION table for the pipeline module.
// `include!`d by both the host crate and the on-device module — one source of
// truth. Lets one pipeline instance hold several versions of its program at once
// (blue/green, canary) and pick one PER RECORD, so a fleet can run mixed versions
// without separate deployments: a request pins a version by tag (the
// `X-Module-Version` header, threaded into a reserved record field), or gets the
// default. An unknown/unloaded version fails CLOSED (never silently runs the wrong
// one). The table is mutable at runtime (hot reload) via `version_apply`.
//
// Each entry also carries a content DIGEST (a sha256 prefix of the program,
// computed by the host control plane). The device resolves by TAG and does not
// verify the digest on the fast path: the digest is the version's content
// identity for the CONTROL PLANE (the release manifest and its fleet
// propagation), where "same tag -> same digest -> same bytecode on every
// instance" is what makes a mixed-version fleet consistent. See
// `chronicle-authoring::release`.

/// Maximum versions held at once (bounded state).
pub const MAX_VERSIONS: usize = 8;
/// Short content-digest length used to identify a version.
pub const VERSION_DIGEST_LEN: usize = 8;
/// Maximum tag length (`v1`, `green`, `canary`, …).
pub const VERSION_TAG_CAP: usize = 24;
/// The reserved record field carrying the per-request version selector (the
/// `X-Module-Version` value). Data fields are 1..=N; 255 is the envelope slot.
pub const VERSION_SELECTOR_FIELD: u32 = 255;

/// One version: a content digest, a human tag, and the program it runs (the
/// pipeline's stage container). All borrow the backing table bytes.
#[derive(Clone, Copy)]
pub struct VersionEntry<'a> {
    pub digest: &'a [u8],
    pub tag: &'a [u8],
    pub prog: &'a [u8],
}

/// A parsed version table borrowing its backing bytes.
///
/// Wire format (the `versions` param, also the hot-reload backing buffer):
/// ```text
///   [nvers:u8][default_idx:u8]
///   nvers × [digest:8][tag_len:u8][tag][prog_len:u16 LE][prog]
/// ```
pub struct VersionTable<'a> {
    default_idx: usize,
    entries: [VersionEntry<'a>; MAX_VERSIONS],
    n: usize,
}

impl<'a> VersionTable<'a> {
    pub fn len(&self) -> usize {
        self.n
    }
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }
    pub fn default_index(&self) -> usize {
        self.default_idx
    }
    pub fn entry(&self, i: usize) -> Option<&VersionEntry<'a>> {
        if i < self.n {
            Some(&self.entries[i])
        } else {
            None
        }
    }
    /// Resolve a request's version selector to an entry index. An empty selector
    /// takes the default; a tag matches by exact bytes; anything else is `None`
    /// (fail closed — the caller emits "version unavailable").
    pub fn resolve(&self, selector: &[u8]) -> Option<usize> {
        if selector.is_empty() {
            return (self.default_idx < self.n).then_some(self.default_idx);
        }
        let mut i = 0;
        while i < self.n {
            if self.entries[i].tag == selector {
                return Some(i);
            }
            i += 1;
        }
        None
    }
}

/// Parse a version table from its backing bytes. `None` on any malformed field.
pub fn parse_version_table(bin: &[u8]) -> Option<VersionTable<'_>> {
    if bin.len() < 2 {
        return None;
    }
    let nvers = bin[0] as usize;
    let default_idx = bin[1] as usize;
    if nvers > MAX_VERSIONS {
        return None;
    }
    let blank = VersionEntry {
        digest: &[],
        tag: &[],
        prog: &[],
    };
    let mut entries = [blank; MAX_VERSIONS];
    let mut off = 2usize;
    let mut i = 0;
    while i < nvers {
        let digest = bin.get(off..off + VERSION_DIGEST_LEN)?;
        off += VERSION_DIGEST_LEN;
        let tag_len = *bin.get(off)? as usize;
        off += 1;
        let tag = bin.get(off..off + tag_len)?;
        off += tag_len;
        let pl = bin.get(off..off + 2)?;
        off += 2;
        let prog_len = u16::from_le_bytes([pl[0], pl[1]]) as usize;
        let prog = bin.get(off..off + prog_len)?;
        off += prog_len;
        entries[i] = VersionEntry { digest, tag, prog };
        i += 1;
    }
    Some(VersionTable {
        default_idx,
        entries,
        n: nvers,
    })
}

/// Load-time validation of every version's stage container in a version
/// table (see `scan_stage_container`) — run at init and after each hot-reload
/// apply, so a table that carries a program this build cannot run is refused
/// as a whole rather than serving some versions and silently failing others.
pub fn scan_version_table(bin: &[u8]) -> Result<(), EvalError> {
    let vt = parse_version_table(bin).ok_or(EvalError::Truncated)?;
    let mut i = 0;
    while let Some(e) = vt.entry(i) {
        scan_stage_container(e.prog)?;
        i += 1;
    }
    Ok(())
}

/// Write one version entry — `[digest:8][tag_len:u8][tag][prog_len:u16 LE][prog]`
/// — into `dst` at `off`, returning the new offset. THE single writer for the
/// entry layout `parse_version_table` reads: both `version_apply` (device) and the
/// host `chronicle-authoring::release` build entries through it, so the format
/// lives in one place. `None` on overflow or an out-of-range tag/program length.
pub fn write_version_entry(
    dst: &mut [u8],
    off: usize,
    digest: &[u8],
    tag: &[u8],
    prog: &[u8],
) -> Option<usize> {
    if digest.len() != VERSION_DIGEST_LEN || tag.len() > 255 || prog.len() > u16::MAX as usize {
        return None;
    }
    let end = off + VERSION_DIGEST_LEN + 1 + tag.len() + 2 + prog.len();
    if end > dst.len() {
        return None;
    }
    let mut w = off;
    dst[w..w + VERSION_DIGEST_LEN].copy_from_slice(digest);
    w += VERSION_DIGEST_LEN;
    dst[w] = tag.len() as u8;
    w += 1;
    dst[w..w + tag.len()].copy_from_slice(tag);
    w += tag.len();
    dst[w..w + 2].copy_from_slice(&(prog.len() as u16).to_le_bytes());
    w += 2;
    dst[w..w + prog.len()].copy_from_slice(prog);
    w += prog.len();
    Some(w)
}

/// Pull the version selector (the `X-Module-Version` value) out of a decoded
/// record's fields — the reserved field 255, if present — else an empty slice.
pub fn version_selector<'a>(fields: &[Field<'a>]) -> &'a [u8] {
    let mut i = 0;
    while i < fields.len() {
        if fields[i].number == VERSION_SELECTOR_FIELD {
            return match fields[i].value {
                Value::Bytes(b) => b,
                Value::Str(s) => s.as_bytes(),
                _ => &[],
            };
        }
        i += 1;
    }
    &[]
}

/// Hot-reload control op-codes (applied to the backing buffer at runtime, over
/// the module's `ctrl_input` port). Non-disruptive: in-flight records keep the
/// version they already resolved; only the table changes.
pub mod vctl {
    pub const ADD_VERSION: u8 = 0x01; // [digest:8][tag_len:u8][tag][prog_len:u16 LE][prog] — append (or replace same-tag)
    pub const SET_DEFAULT: u8 = 0x02; // [tag_len:u8][tag] — repoint the default to this tag (blue-green flip)
    pub const REMOVE_VERSION: u8 = 0x03; // [tag_len:u8][tag] — drop a version (reclaim a slot)
                                         // Same as ADD_VERSION, but the program is an IR-stages container the target
                                         // LOWERS at apply-time (`version_apply_ir`), so a running instance re-derives
                                         // the bytecode itself instead of trusting a pre-lowered program — the same
                                         // self-validation the `ir_stages` param gets at module load, now for hot reload.
    pub const ADD_VERSION_IR: u8 = 0x04; // [digest:8][tag_len:u8][tag][ir_len:u16 LE][ir_stages]
}

/// Deterministic hot-reload outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionCtlError {
    BadMessage,
    TableFull,
    UnknownTag,
    Overflow,
}

/// Find the entry index whose tag matches `tag`, scanning `bin` in place.
fn find_tag(bin: &[u8], tag: &[u8]) -> Option<usize> {
    let table = parse_version_table(bin)?;
    let mut i = 0;
    while i < table.n {
        if table.entries[i].tag == tag {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Apply one control message to the version table backing buffer `bin` (length
/// `used`, capacity `cap`). Returns the new used length. Mutates in place; the
/// format stays parseable after every op.
pub fn version_apply(
    bin: &mut [u8],
    used: usize,
    cap: usize,
    msg: &[u8],
) -> Result<usize, VersionCtlError> {
    let op = *msg.first().ok_or(VersionCtlError::BadMessage)?;
    match op {
        vctl::ADD_VERSION => {
            // Parse the entry out of the message body.
            let body = &msg[1..];
            let digest = body
                .get(0..VERSION_DIGEST_LEN)
                .ok_or(VersionCtlError::BadMessage)?;
            let tag_len = *body
                .get(VERSION_DIGEST_LEN)
                .ok_or(VersionCtlError::BadMessage)? as usize;
            let ts = VERSION_DIGEST_LEN + 1;
            let tag = body
                .get(ts..ts + tag_len)
                .ok_or(VersionCtlError::BadMessage)?;
            let ps = ts + tag_len;
            let pl = body.get(ps..ps + 2).ok_or(VersionCtlError::BadMessage)?;
            let prog_len = u16::from_le_bytes([pl[0], pl[1]]) as usize;
            let prog = body
                .get(ps + 2..ps + 2 + prog_len)
                .ok_or(VersionCtlError::BadMessage)?;

            // Replace an existing same-tag entry by dropping it first (so a
            // redeploy of a tag swaps its program rather than duplicating).
            let used = match find_tag(&bin[..used], tag) {
                Some(_) => remove_tag(bin, used, tag)?,
                None => used,
            };

            if bin[0] as usize >= MAX_VERSIONS {
                return Err(VersionCtlError::TableFull);
            }
            // Append via the shared writer (`cap` bounds the buffer), then count it.
            let w = write_version_entry(&mut bin[..cap], used, digest, tag, prog)
                .ok_or(VersionCtlError::Overflow)?;
            bin[0] += 1;
            Ok(w)
        }
        vctl::SET_DEFAULT => {
            let tag_len = *msg.get(1).ok_or(VersionCtlError::BadMessage)? as usize;
            let tag = msg.get(2..2 + tag_len).ok_or(VersionCtlError::BadMessage)?;
            let idx = find_tag(&bin[..used], tag).ok_or(VersionCtlError::UnknownTag)?;
            bin[1] = idx as u8;
            Ok(used)
        }
        vctl::REMOVE_VERSION => {
            let tag_len = *msg.get(1).ok_or(VersionCtlError::BadMessage)? as usize;
            let tag = msg.get(2..2 + tag_len).ok_or(VersionCtlError::BadMessage)?;
            remove_tag(bin, used, tag)
        }
        _ => Err(VersionCtlError::BadMessage),
    }
}

/// Apply an `ADD_VERSION_IR` control message: like `ADD_VERSION`, but the
/// message body carries an IR-stages container that is LOWERED into the table's
/// program slot here, at apply-time, via `lower_stages`. So a hot reload into a
/// running instance ships the checked IR and the target re-derives the bytecode
/// itself — a version that will not lower is rejected (`Overflow`/`BadMessage`)
/// rather than stored, exactly as an unrunnable `ir_stages` param fails at load.
///
/// The IR lowers straight into `bin` at the program offset (no intermediate
/// buffer): the header is written afterward with the re-derived program length.
pub fn version_apply_ir(
    bin: &mut [u8],
    used: usize,
    cap: usize,
    msg: &[u8],
) -> Result<usize, VersionCtlError> {
    if *msg.first().ok_or(VersionCtlError::BadMessage)? != vctl::ADD_VERSION_IR {
        return Err(VersionCtlError::BadMessage);
    }
    let body = &msg[1..];
    let digest = body
        .get(0..VERSION_DIGEST_LEN)
        .ok_or(VersionCtlError::BadMessage)?;
    let tag_len = *body
        .get(VERSION_DIGEST_LEN)
        .ok_or(VersionCtlError::BadMessage)? as usize;
    let ts = VERSION_DIGEST_LEN + 1;
    let tag = body
        .get(ts..ts + tag_len)
        .ok_or(VersionCtlError::BadMessage)?;
    let ps = ts + tag_len;
    let il = body.get(ps..ps + 2).ok_or(VersionCtlError::BadMessage)?;
    let ir_len = u16::from_le_bytes([il[0], il[1]]) as usize;
    let ir_prog = body
        .get(ps + 2..ps + 2 + ir_len)
        .ok_or(VersionCtlError::BadMessage)?;

    // Replace an existing same-tag entry first (redeploy swaps, not duplicates).
    let used = match find_tag(&bin[..used], tag) {
        Some(_) => remove_tag(bin, used, tag)?,
        None => used,
    };
    if bin[0] as usize >= MAX_VERSIONS {
        return Err(VersionCtlError::TableFull);
    }

    // Entry layout at `used`: [digest:8][tag_len:1][tag][prog_len:2][prog]. Lower
    // the IR straight into the prog slot, then backfill the header (digest/tag
    // borrow `msg`, disjoint from `bin`, so this aliases nothing).
    let hdr = VERSION_DIGEST_LEN + 1 + tag_len + 2;
    let prog_start = used + hdr;
    if prog_start > cap {
        return Err(VersionCtlError::Overflow);
    }
    let plen =
        lower_stages(ir_prog, &mut bin[prog_start..cap]).map_err(|_| VersionCtlError::Overflow)?;
    if plen > u16::MAX as usize {
        return Err(VersionCtlError::Overflow);
    }
    let mut w = used;
    bin[w..w + VERSION_DIGEST_LEN].copy_from_slice(digest);
    w += VERSION_DIGEST_LEN;
    bin[w] = tag_len as u8;
    w += 1;
    bin[w..w + tag_len].copy_from_slice(tag);
    w += tag_len;
    bin[w..w + 2].copy_from_slice(&(plen as u16).to_le_bytes());
    bin[0] += 1;
    Ok(prog_start + plen)
}

/// Remove the entry with `tag`, compacting the buffer and fixing count/default.
fn remove_tag(bin: &mut [u8], used: usize, tag: &[u8]) -> Result<usize, VersionCtlError> {
    // Locate the entry's byte range by re-walking the header.
    let nvers = bin[0] as usize;
    let mut off = 2usize;
    let mut hit: Option<(usize, usize, usize)> = None; // (index, start, end)
    let mut i = 0;
    while i < nvers {
        let start = off;
        let d = VERSION_DIGEST_LEN;
        let tl = *bin.get(off + d).ok_or(VersionCtlError::BadMessage)? as usize;
        let ts = off + d + 1;
        let this_tag = bin.get(ts..ts + tl).ok_or(VersionCtlError::BadMessage)?;
        let ps = ts + tl;
        let pl = bin.get(ps..ps + 2).ok_or(VersionCtlError::BadMessage)?;
        let prog_len = u16::from_le_bytes([pl[0], pl[1]]) as usize;
        let end = ps + 2 + prog_len;
        if this_tag == tag {
            hit = Some((i, start, end));
        }
        off = end;
        i += 1;
    }
    let (idx, start, end) = hit.ok_or(VersionCtlError::UnknownTag)?;
    // Compact: shift the tail left over the removed entry.
    let tail = end..used;
    let shift = end - start;
    let mut k = start;
    for j in tail {
        bin[k] = bin[j];
        k += 1;
    }
    bin[0] = (nvers - 1) as u8;
    // Fix the default pointer: unchanged if before, shift down if after, and
    // clamp to 0 if it pointed at the removed entry.
    let def = bin[1] as usize;
    bin[1] = if def == idx {
        0
    } else if def > idx {
        (def - 1) as u8
    } else {
        def as u8
    };
    Ok(used - shift)
}

/// One version to publish: its selector tag, its program, and that program's
/// content digest.
///
/// The digest is supplied rather than computed here on purpose. This core is
/// mounted by a harness that FORBIDS unsafe code, and the SDK crypto is
/// verbatim unsafe source — so hashing stays with the caller, which already
/// owns a sha256, and this file stays pure.
#[derive(Clone, Copy)]
pub struct VersionSpec<'a> {
    pub tag: &'a [u8],
    pub program: &'a [u8],
    pub digest: [u8; VERSION_DIGEST_LEN],
}

/// Build the `versions` param a pipeline module loads at startup:
/// `[nvers:u8][default_idx:u8]` then one entry per version.
///
/// The device could already READ and APPLY a version table; this is the other
/// half — AUTHORING one. Without it a node can serve multiple versions but
/// cannot decide what they are, which leaves the release model dependent on a
/// build host.
///
/// `default_tag` selects which version unselected traffic gets; an unknown tag
/// falls back to index 0, matching the host rather than failing a deployment
/// over a typo in a default.
pub fn build_versions_param(
    versions: &[VersionSpec],
    default_tag: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    if versions.is_empty() || versions.len() > u8::MAX as usize {
        return None;
    }
    let mut default_idx = 0usize;
    for (i, v) in versions.iter().enumerate() {
        if v.tag == default_tag {
            default_idx = i;
            break;
        }
    }
    if out.len() < 2 {
        return None;
    }
    out[0] = versions.len() as u8;
    out[1] = default_idx as u8;
    let mut p = 2usize;
    for v in versions {
        let need = VERSION_DIGEST_LEN + 1 + v.tag.len() + 2 + v.program.len();
        if p + need > out.len() {
            return None;
        }
        write_version_entry(out, p, &v.digest, v.tag, v.program);
        p += need;
    }
    Some(p)
}
