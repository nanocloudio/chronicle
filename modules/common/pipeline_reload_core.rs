// Transactional hot-reload for the pipeline version table, kept out of the ABI shell
// so its "a rejected candidate never touches the active generation" property is a
// host test rather than a claim. Mounted after version_core (for
// `version_apply`/`version_apply_ir`/`scan_version_table`/`vctl`) and outcome_core.
//
// The predecessor applied the control op into the LIVE table and only then scanned
// it, faulting the running node when a bad program slipped through — a rejected
// update could destroy a healthy instance. Here the op is applied to a CANDIDATE
// copy and fully scanned; the caller copies the candidate over the active table
// ONLY on `Ok`. On any error the active table is byte-identical and the caller
// counts a rejection.

/// Apply a hot-reload control message to a candidate copy of `active` and validate
/// it. Returns the new table length (the caller then copies `cand[..len]` over the
/// active table) or a `Reason` — in which case `cand` may be dirty but the active
/// table the caller still holds is untouched.
pub fn pipeline_reload(
    active: &[u8],
    cand: &mut [u8],
    cap: usize,
    msg: &[u8],
) -> Result<usize, Reason> {
    if active.len() > cap || active.len() > cand.len() {
        return Err(Reason::Capacity);
    }
    cand[..active.len()].copy_from_slice(active);
    let applied = if msg.first() == Some(&vctl::ADD_VERSION_IR) {
        version_apply_ir(cand, active.len(), cap, msg)
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
    if scan_version_table(&cand[..nu]).is_err() {
        return Err(Reason::Unsupported);
    }
    Ok(nu)
}
