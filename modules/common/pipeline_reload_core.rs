// Transactional hot-reload for the pipeline version table, kept out of the ABI shell
// so its "a rejected candidate never touches the active generation" property is a
// host test rather than a claim. Mounted after version_core (for
// `version_apply`/`version_apply_ir`/`scan_version_table`/`vctl`), lower_core
// (`lower_stages_kinded`) and outcome_core.
//
// The control op is applied to a CANDIDATE copy of the active table and fully
// scanned; the caller copies the candidate over the active table ONLY on `Ok`. On
// any error the active table is byte-identical and the caller counts a rejection.

/// Apply a hot-reload control message to a candidate copy of `active` and validate
/// it. Returns the new table length (the caller then copies `cand[..len]` over the
/// active table) or a `Reason` — in which case `cand` may be dirty but the active
/// table the caller still holds is untouched.
///
/// Every stage is compute; a table with decision or map stages reloads through
/// [`pipeline_reload_kinded`].
pub fn pipeline_reload(
    active: &[u8],
    cand: &mut [u8],
    cap: usize,
    msg: &[u8],
) -> Result<usize, Reason> {
    pipeline_reload_kinded(active, cand, cap, msg, &[], |t: &[u8]| {
        scan_version_table(t).is_ok()
    })
}

/// [`pipeline_reload`] for a table whose stages carry KINDS (`kinds`, parallel
/// to each version's stage container, as at load). An `ADD_VERSION_IR` body is
/// lowered kinded — a decision or map body is carried verbatim, never lowered as
/// expression IR — and the applied table must pass `runnable`, the caller's
/// scan of each stage by the scanner its kind names (the decision scanner lives
/// with the decision executor, which this core does not mount).
pub fn pipeline_reload_kinded<F: Fn(&[u8]) -> bool>(
    active: &[u8],
    cand: &mut [u8],
    cap: usize,
    msg: &[u8],
    kinds: &[u8],
    runnable: F,
) -> Result<usize, Reason> {
    if active.len() > cap || active.len() > cand.len() {
        return Err(Reason::Capacity);
    }
    cand[..active.len()].copy_from_slice(active);
    let applied = if msg.first() == Some(&vctl::ADD_VERSION_IR) {
        if kinds.is_empty() {
            version_apply_ir(cand, active.len(), cap, msg)
        } else {
            version_apply_ir_kinded(cand, active.len(), cap, msg, kinds)
        }
    } else {
        version_apply(cand, active.len(), cap, msg)
    };
    let nu = match applied {
        Ok(nu) => nu,
        // Malformed control message or table overflow: reject, active untouched.
        Err(_) => return Err(Reason::Malformed),
    };
    // The applied table must be runnable in THIS build — an added program needing a
    // builtin this build lacks is rejected, not faulted onto the live node.
    if !runnable(&cand[..nu]) {
        return Err(Reason::Unsupported);
    }
    Ok(nu)
}

/// `version_apply_ir` with the stage kinds applied to the lowering
/// (`lower_stages_kinded`). The entry layout and the same-tag replacement are
/// `version_apply_ir`'s.
fn version_apply_ir_kinded(
    bin: &mut [u8],
    used: usize,
    cap: usize,
    msg: &[u8],
    kinds: &[u8],
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
    let used = match find_tag(&bin[..used], tag) {
        Some(_) => remove_tag(bin, used, tag)?,
        None => used,
    };
    if bin[0] as usize >= MAX_VERSIONS {
        return Err(VersionCtlError::TableFull);
    }
    // [digest:8][tag_len:1][tag][prog_len:2][prog]: lower into the prog slot,
    // then backfill the header.
    let hdr = VERSION_DIGEST_LEN + 1 + tag_len + 2;
    let prog_start = used + hdr;
    if prog_start > cap || cap > bin.len() {
        return Err(VersionCtlError::Overflow);
    }
    let plen = lower_stages_kinded(ir_prog, kinds, &mut bin[prog_start..cap])
        .map_err(|_| VersionCtlError::Overflow)?;
    if plen > u16::MAX as usize {
        return Err(VersionCtlError::Overflow);
    }
    let mut w = used;
    for (dst, src) in bin[w..].iter_mut().zip(digest) {
        *dst = *src;
    }
    w += VERSION_DIGEST_LEN;
    bin[w] = tag_len as u8;
    w += 1;
    for (dst, src) in bin[w..].iter_mut().zip(tag).take(tag_len) {
        *dst = *src;
    }
    w += tag_len;
    bin[w..w + 2].copy_from_slice(&(plen as u16).to_le_bytes());
    bin[0] += 1;
    Ok(prog_start + plen)
}
