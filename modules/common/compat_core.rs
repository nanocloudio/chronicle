// Semantic + structural diffing, on device — deciding whether a new version of
// a module or a schema can replace the old one.
//
// This is what a node consults before it swaps something out. Activation asks
// "can I run this?"; compatibility asks "can I run this INSTEAD, without
// breaking whoever depends on the thing it replaces?". A node that cannot answer
// that has to treat every update as either safe (and sometimes break callers) or
// unsafe (and never update).
//
// Two verdicts, both mirroring the host exactly:
//
//   MODULE   — IDENTICAL when the recomputed digests match. BREAKING when an
//              entry point disappears (callers lose a surface) or the new
//              version requires a capability the old one did not (a stricter
//              demand on the runtime, so a node that ran the old may not run the
//              new). Otherwise COMPATIBLE: additive.
//   SCHEMA   — IDENTICAL when the descriptor digests match. BREAKING when a
//              message vanishes, or a field is removed, renumbered, or changes
//              type or label. Otherwise COMPATIBLE: fields may be ADDED.
//
// The schema digest is the subtle part. The host hashes a SORTED copy — files by
// name, messages within a file by name — so two descriptor sets that differ only
// in ordering are Identical to it. Comparing raw bytes here would call those
// Compatible instead, a quiet disagreement on exactly the case the sorting
// exists to handle. So `schema_descriptor_digest` rebuilds the sorted encoding
// before hashing, permuting whole encoded regions rather than re-encoding their
// contents.
//
// That permutation assumes the input is canonical protobuf (fields ascending by
// number), which is what every producer in this system emits — the same
// assumption `modsig_core`'s strip makes.
//
// Requires `pb_core` and `modsig_core`, plus the fluxor SDK `sha256`.

/// `CompatibilityLevel` (proto/unified/v1/common.proto).
pub const COMPAT_UNSPECIFIED: i32 = 0;
pub const COMPAT_IDENTICAL: i32 = 1;
pub const COMPAT_COMPATIBLE: i32 = 2;
pub const COMPAT_BREAKING: i32 = 3;

/// Module: `capabilities = 11`, `entry_points = 12`.
const F_ENTRY_POINTS: u32 = 12;
/// `EntryPoint { name = 1, pipeline = 2 }`.
const F_EP_NAME: u32 = 1;
/// `FileDescriptorSet { file = 1 }`.
const F_FDS_FILE: u32 = 1;
/// `FileDescriptorProto { name = 1, package = 2, message_type = 4 }`.
const F_FD_NAME: u32 = 1;
const F_FD_PACKAGE: u32 = 2;
const F_FD_MESSAGE_TYPE: u32 = 4;
/// `DescriptorProto { name = 1, field = 2 }`.
const F_DP_NAME: u32 = 1;
const F_DP_FIELD: u32 = 2;
/// `FieldDescriptorProto { name = 1, number = 3, label = 4, type = 5, type_name = 6 }`.
const F_FIELD_NUMBER: u32 = 3;
const F_FIELD_LABEL: u32 = 4;
const F_FIELD_TYPE: u32 = 5;
const F_FIELD_TYPE_NAME: u32 = 6;

/// Bounds on what one descriptor set may hold. Exceeding either is reported
/// rather than truncated — a diff computed over part of a schema would be a
/// confident wrong answer.
pub const MAX_FILES: usize = 32;
pub const MAX_MSGS_PER_FILE: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatError {
    /// The bytes are not a well-formed message.
    Malformed,
    /// A caller buffer was too small.
    TooLarge,
    /// More files or messages than the bounded tables hold.
    TooMany,
}

impl From<PbError> for CompatError {
    fn from(e: PbError) -> Self {
        match e {
            PbError::Overflow => CompatError::TooLarge,
            _ => CompatError::Malformed,
        }
    }
}

impl From<VerifyError> for CompatError {
    fn from(e: VerifyError) -> Self {
        match e {
            VerifyError::TooLarge => CompatError::TooLarge,
            _ => CompatError::Malformed,
        }
    }
}

// ------------------------------------------------------------------ modules

/// Whether `name` appears as an entry-point name in `module`.
fn has_entry_point(module: &[u8], name: &[u8]) -> Result<bool, CompatError> {
    let mut r = PbR::new(module);
    while let Some(f) = r.next_field()? {
        if f.number != F_ENTRY_POINTS || f.wire != WT_LEN {
            continue;
        }
        let mut er = PbR::new(f.bytes);
        while let Some(ef) = er.next_field()? {
            if ef.number == F_EP_NAME && ef.wire == WT_LEN && ef.bytes == name {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Whether `name` is a REQUIRED capability of `module`.
fn requires_capability(module: &[u8], name: &[u8]) -> Result<bool, CompatError> {
    let mut r = PbR::new(module);
    while let Some(f) = r.next_field()? {
        if f.number != F_CAPABILITIES || f.wire != WT_LEN {
            continue;
        }
        let mut cname: &[u8] = &[];
        let mut optional = false;
        let mut cr = PbR::new(f.bytes);
        while let Some(cf) = cr.next_field()? {
            match cf.number {
                F_CAP_NAME if cf.wire == WT_LEN => cname = cf.bytes,
                F_CAP_OPTIONAL if cf.wire == WT_VARINT => optional = cf.value != 0,
                _ => {}
            }
        }
        if !optional && cname == name {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Compare two sealed Modules.
///
/// `scratch_a`/`scratch_b` each hold one module's pre-digest reconstruction and
/// must be at least as large as the module they serve.
pub fn module_compatibility(
    old: &[u8],
    new: &[u8],
    scratch_a: &mut [u8],
    scratch_b: &mut [u8],
) -> Result<i32, CompatError> {
    // Identity is by recomputed digest, not by the carried one — the same
    // reasoning as verification: a module that merely CLAIMS a digest proves
    // nothing about its contents.
    let da = module_recomputed_digest(old, scratch_a)?;
    let db = module_recomputed_digest(new, scratch_b)?;
    if da == db {
        return Ok(COMPAT_IDENTICAL);
    }

    // A removed entry point takes away a surface someone may be calling.
    let mut r = PbR::new(old);
    while let Some(f) = r.next_field()? {
        if f.number != F_ENTRY_POINTS || f.wire != WT_LEN {
            continue;
        }
        let mut er = PbR::new(f.bytes);
        while let Some(ef) = er.next_field()? {
            if ef.number == F_EP_NAME && ef.wire == WT_LEN && !has_entry_point(new, ef.bytes)? {
                return Ok(COMPAT_BREAKING);
            }
        }
    }

    // A newly REQUIRED capability is a stricter demand on the runtime: a node
    // that ran the old version may not be able to run this one.
    let mut r = PbR::new(new);
    while let Some(f) = r.next_field()? {
        if f.number != F_CAPABILITIES || f.wire != WT_LEN {
            continue;
        }
        let mut cname: &[u8] = &[];
        let mut optional = false;
        let mut cr = PbR::new(f.bytes);
        while let Some(cf) = cr.next_field()? {
            match cf.number {
                F_CAP_NAME if cf.wire == WT_LEN => cname = cf.bytes,
                F_CAP_OPTIONAL if cf.wire == WT_VARINT => optional = cf.value != 0,
                _ => {}
            }
        }
        if !optional && !requires_capability(old, cname)? {
            return Ok(COMPAT_BREAKING);
        }
    }

    Ok(COMPAT_COMPATIBLE)
}

// ------------------------------------------------------------------ schemas

/// The value of a length-delimited sub-field, if present.
fn sub_bytes(msg: &[u8], field: u32) -> Result<&[u8], CompatError> {
    let mut r = PbR::new(msg);
    while let Some(f) = r.next_field()? {
        if f.number == field && f.wire == WT_LEN {
            return Ok(f.bytes);
        }
    }
    Ok(&[])
}

/// The value of a varint sub-field, or 0.
fn sub_varint(msg: &[u8], field: u32) -> Result<u64, CompatError> {
    let mut r = PbR::new(msg);
    while let Some(f) = r.next_field()? {
        if f.number == field && f.wire == WT_VARINT {
            return Ok(f.value);
        }
    }
    Ok(0)
}

/// Order `idx[..n]` so the referenced names sort ascending. An insertion sort:
/// `n` is bounded and small, and it needs no allocation.
fn sort_by_name(idx: &mut [usize], n: usize, names: &[&[u8]]) {
    let mut i = 1;
    while i < n {
        let mut j = i;
        while j > 0 && names[idx[j - 1]] > names[idx[j]] {
            idx.swap(j - 1, j);
            j -= 1;
        }
        i += 1;
    }
}

/// Rebuild one `FileDescriptorProto` with its `message_type` entries sorted by
/// name, into `w`.
fn write_sorted_file(w: &mut Pb, file: &[u8]) -> Result<(), CompatError> {
    // Collect the message_type regions and their names.
    let mut raws = [(&[] as &[u8], &[] as &[u8]); MAX_MSGS_PER_FILE];
    let mut count = 0usize;
    let mut r = PbR::new(file);
    while let Some(f) = r.next_field()? {
        if f.number == F_FD_MESSAGE_TYPE && f.wire == WT_LEN {
            if count == MAX_MSGS_PER_FILE {
                return Err(CompatError::TooMany);
            }
            raws[count] = (sub_bytes(f.bytes, F_DP_NAME)?, f.raw);
            count += 1;
        }
    }
    let mut names = [&[] as &[u8]; MAX_MSGS_PER_FILE];
    let mut idx = [0usize; MAX_MSGS_PER_FILE];
    for k in 0..count {
        names[k] = raws[k].0;
        idx[k] = k;
    }
    sort_by_name(&mut idx, count, &names);

    // Emit fields in encounter order; at the FIRST message_type, emit all of
    // them in sorted order and skip the rest. Canonical protobuf orders fields
    // ascending, so the repeated entries are contiguous and this preserves the
    // overall field order.
    let mut emitted_msgs = false;
    let mut r = PbR::new(file);
    while let Some(f) = r.next_field()? {
        if f.number == F_FD_MESSAGE_TYPE && f.wire == WT_LEN {
            if !emitted_msgs {
                for k in 0..count {
                    w.raw(raws[idx[k]].1)?;
                }
                emitted_msgs = true;
            }
            continue;
        }
        w.raw(f.raw)?;
    }
    Ok(())
}

/// The host's `descriptor_digest`: sha256 over the descriptor set with files
/// sorted by name and each file's messages sorted by name.
pub fn schema_descriptor_digest(fds: &[u8], scratch: &mut [u8]) -> Result<[u8; 32], CompatError> {
    let mut raws = [(&[] as &[u8], &[] as &[u8]); MAX_FILES];
    let mut count = 0usize;
    let mut r = PbR::new(fds);
    while let Some(f) = r.next_field()? {
        if f.number == F_FDS_FILE && f.wire == WT_LEN {
            if count == MAX_FILES {
                return Err(CompatError::TooMany);
            }
            raws[count] = (sub_bytes(f.bytes, F_FD_NAME)?, f.bytes);
            count += 1;
        }
    }
    let mut names = [&[] as &[u8]; MAX_FILES];
    let mut idx = [0usize; MAX_FILES];
    for k in 0..count {
        names[k] = raws[k].0;
        idx[k] = k;
    }
    sort_by_name(&mut idx, count, &names);

    let mut w = Pb::new(scratch);
    for k in 0..count {
        let m = w.open(F_FDS_FILE)?;
        write_sorted_file(&mut w, raws[idx[k]].1)?;
        w.close(m)?;
    }
    let n = w.len();
    Ok(sha256(&scratch[..n]))
}

/// Find the message named `pkg`.`name` in `fds`, returning its `DescriptorProto`.
fn find_message<'a>(
    fds: &'a [u8],
    pkg: &[u8],
    name: &[u8],
) -> Result<Option<&'a [u8]>, CompatError> {
    let mut r = PbR::new(fds);
    while let Some(f) = r.next_field()? {
        if f.number != F_FDS_FILE || f.wire != WT_LEN {
            continue;
        }
        if sub_bytes(f.bytes, F_FD_PACKAGE)? != pkg {
            continue;
        }
        let mut fr = PbR::new(f.bytes);
        while let Some(mf) = fr.next_field()? {
            if mf.number == F_FD_MESSAGE_TYPE
                && mf.wire == WT_LEN
                && sub_bytes(mf.bytes, F_DP_NAME)? == name
            {
                return Ok(Some(mf.bytes));
            }
        }
    }
    Ok(None)
}

/// Compare two descriptor closures.
///
/// Each scratch buffer holds one sorted re-encoding and must be at least as
/// large as the descriptor set it serves.
pub fn schema_compatibility(
    old: &[u8],
    new: &[u8],
    scratch_a: &mut [u8],
    scratch_b: &mut [u8],
) -> Result<i32, CompatError> {
    if schema_descriptor_digest(old, scratch_a)? == schema_descriptor_digest(new, scratch_b)? {
        return Ok(COMPAT_IDENTICAL);
    }

    // Every message in `old` must still exist, and every field it declared must
    // be preserved by number, type, type_name and label. Additions are fine.
    let mut r = PbR::new(old);
    while let Some(f) = r.next_field()? {
        if f.number != F_FDS_FILE || f.wire != WT_LEN {
            continue;
        }
        let pkg = sub_bytes(f.bytes, F_FD_PACKAGE)?;
        let mut fr = PbR::new(f.bytes);
        while let Some(mf) = fr.next_field()? {
            if mf.number != F_FD_MESSAGE_TYPE || mf.wire != WT_LEN {
                continue;
            }
            let mname = sub_bytes(mf.bytes, F_DP_NAME)?;
            let Some(nm) = find_message(new, pkg, mname)? else {
                return Ok(COMPAT_BREAKING); // message removed
            };
            let mut dr = PbR::new(mf.bytes);
            while let Some(df) = dr.next_field()? {
                if df.number != F_DP_FIELD || df.wire != WT_LEN {
                    continue;
                }
                let num = sub_varint(df.bytes, F_FIELD_NUMBER)?;
                let Some(nf) = find_field(nm, num)? else {
                    return Ok(COMPAT_BREAKING); // field removed or renumbered
                };
                if sub_varint(nf, F_FIELD_TYPE)? != sub_varint(df.bytes, F_FIELD_TYPE)?
                    || sub_varint(nf, F_FIELD_LABEL)? != sub_varint(df.bytes, F_FIELD_LABEL)?
                    || sub_bytes(nf, F_FIELD_TYPE_NAME)? != sub_bytes(df.bytes, F_FIELD_TYPE_NAME)?
                {
                    return Ok(COMPAT_BREAKING); // type or label changed
                }
            }
        }
    }
    Ok(COMPAT_COMPATIBLE)
}

/// The field with number `num` in a `DescriptorProto`.
fn find_field(msg: &[u8], num: u64) -> Result<Option<&[u8]>, CompatError> {
    let mut r = PbR::new(msg);
    while let Some(f) = r.next_field()? {
        if f.number == F_DP_FIELD && f.wire == WT_LEN && sub_varint(f.bytes, F_FIELD_NUMBER)? == num
        {
            return Ok(Some(f.bytes));
        }
    }
    Ok(None)
}
