// The OCI image layout, on device — `oci-layout` + `blobs/sha256/<hex>` +
// `index.json`, written and read with no allocator and no std.
//
// The distribution half: a node that SEALS an artefact (`artefact_core`) and
// KEEPS it (`blobstore_core`) PUBLISHES it here, in the layout every OCI
// registry already speaks, so a bundle pushed from a device is served unchanged
// by `registry:2`, zot, or fluxor's own store — no translation step.
//
// INTEROP IS THE POINT, and it constrains the parser: `index.json` here may have
// been written by the host (`chronicle-module/src/oci.rs`, via serde_json's
// pretty printer) or by a registry. So the reader below is a real JSON scanner —
// arbitrary whitespace, nesting, and escapes — not a pattern match against our
// own writer's output. The writer is deliberately compact and deterministic;
// the reader assumes nothing about layout.
//
// Content addressing stays honest: blobs are fetched through
// `blobstore_core::blob_get`, which rehashes and refuses a mismatch. `index.json`
// and `oci-layout` are NAMED objects (mutable pointers), so they go through
// `put_named`/`get_named` and carry no such guarantee — by nature, since a tag
// is meant to move.
//
// Requires `blobstore_core`, the SDK `sha256`, and a `SyscallTable`.

/// Media types — the `application/vnd.nanocloud.*` convention, matching the host
/// so a bundle round-trips between them byte for byte.
pub const MEDIA_TYPE_MODULE: &[u8] = b"application/vnd.nanocloud.unified.module.v1+pb";
pub const MEDIA_TYPE_ARTEFACT: &[u8] = b"application/vnd.nanocloud.unified.artefact.v1+pb";
pub const MEDIA_TYPE_MANIFEST: &[u8] = b"application/vnd.oci.image.manifest.v1+json";
pub const MEDIA_TYPE_EMPTY: &[u8] = b"application/vnd.oci.empty.v1+json";

/// Annotation keys (mirror `io.fluxor.provenance` / `io.fluxor.source-rev`).
pub const ANNOTATION_PROVENANCE: &[u8] = b"io.nanocloud.unified.provenance";
pub const ANNOTATION_SOURCE_REV: &[u8] = b"io.nanocloud.unified.source-rev";
pub const ANNOTATION_NAME: &[u8] = b"org.opencontainers.image.ref.name";

/// Store keys for the two named objects in an image layout.
pub const KEY_LAYOUT: &[u8] = b"oci-layout";
pub const KEY_INDEX: &[u8] = b"index.json";

/// The `oci-layout` marker body — a fixed constant, per the OCI spec.
pub const LAYOUT_BODY: &[u8] = b"{\"imageLayoutVersion\":\"1.0.0\"}";

/// An empty index, written when a store is first opened.
pub const EMPTY_INDEX: &[u8] = b"{\"schemaVersion\":2,\"manifests\":[]}";

/// Most layers one bundle may carry (module + artefacts).
pub const MAX_LAYERS: usize = 32;
/// Most manifest entries `index.json` may hold.
pub const MAX_INDEX_ENTRIES: usize = 64;

/// Why an OCI operation failed. Values, never panics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OciError {
    /// The underlying object store rejected the operation.
    Store,
    /// A referenced blob, tag, or required member is absent.
    NotFound,
    /// A caller buffer cannot hold the result.
    TooLarge,
    /// The JSON is not well formed, or lacks a member the layout requires.
    Malformed,
    /// A blob's bytes do not hash to the digest they were stored under.
    DigestMismatch,
    /// More layers or index entries than the bounded tables hold.
    TooMany,
}

impl From<BlobError> for OciError {
    fn from(e: BlobError) -> Self {
        match e {
            BlobError::PutFailed => OciError::Store,
            BlobError::NotFound => OciError::NotFound,
            BlobError::TooLarge => OciError::TooLarge,
            BlobError::DigestMismatch => OciError::DigestMismatch,
        }
    }
}

// ---------------------------------------------------------------- JSON reader
//
// A cursor-style scanner over a byte slice. Every function takes an offset and
// returns the offset just past what it consumed, so composition needs no state.

fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\n' | b'\r') {
        i += 1;
    }
    i
}

/// Consume a JSON string starting at `b[i] == '"'`. Returns the byte range of
/// the RAW contents (escapes not expanded) and the offset past the close quote.
fn scan_string(b: &[u8], i: usize) -> Option<(usize, usize, usize)> {
    if i >= b.len() || b[i] != b'"' {
        return None;
    }
    let start = i + 1;
    let mut j = start;
    while j < b.len() {
        match b[j] {
            b'\\' => j += 2, // skip the escape and whatever it escapes
            b'"' => return Some((start, j, j + 1)),
            _ => j += 1,
        }
    }
    None
}

/// Consume any JSON value at `i`, returning the offset just past it.
fn skip_value(b: &[u8], i: usize) -> Option<usize> {
    let i = skip_ws(b, i);
    if i >= b.len() {
        return None;
    }
    match b[i] {
        b'"' => scan_string(b, i).map(|(_, _, end)| end),
        b'{' | b'[' => {
            // Track nesting, ignoring braces that appear inside strings.
            let mut depth = 0usize;
            let mut j = i;
            while j < b.len() {
                match b[j] {
                    b'"' => {
                        let (_, _, end) = scan_string(b, j)?;
                        j = end;
                        continue;
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(j + 1);
                        }
                    }
                    _ => {}
                }
                j += 1;
            }
            None
        }
        _ => {
            // number / true / false / null — run to the next structural byte.
            let mut j = i;
            while j < b.len() && !matches!(b[j], b',' | b'}' | b']' | b' ' | b'\n' | b'\t' | b'\r')
            {
                j += 1;
            }
            Some(j)
        }
    }
}

/// Find `key` in the object starting at `i` (`b[i] == '{'`), returning the
/// offset of its VALUE. Only the object's own members are considered — nested
/// objects are skipped wholesale, so a key never matches at the wrong depth.
pub fn obj_find(b: &[u8], i: usize, key: &[u8]) -> Option<usize> {
    let i = skip_ws(b, i);
    if i >= b.len() || b[i] != b'{' {
        return None;
    }
    let mut j = skip_ws(b, i + 1);
    if j < b.len() && b[j] == b'}' {
        return None;
    }
    loop {
        let (ks, ke, after) = scan_string(b, skip_ws(b, j))?;
        let j2 = skip_ws(b, after);
        if j2 >= b.len() || b[j2] != b':' {
            return None;
        }
        let vpos = skip_ws(b, j2 + 1);
        if &b[ks..ke] == key {
            return Some(vpos);
        }
        j = skip_ws(b, skip_value(b, vpos)?);
        if j >= b.len() {
            return None;
        }
        match b[j] {
            b',' => j += 1,
            b'}' => return None,
            _ => return None,
        }
    }
}

/// Offsets of each element of the array starting at `i` (`b[i] == '['`).
/// Returns how many were written into `out`.
pub fn arr_elems(b: &[u8], i: usize, out: &mut [usize]) -> Option<usize> {
    let i = skip_ws(b, i);
    if i >= b.len() || b[i] != b'[' {
        return None;
    }
    let mut n = 0usize;
    let mut j = skip_ws(b, i + 1);
    if j < b.len() && b[j] == b']' {
        return Some(0);
    }
    loop {
        if n == out.len() {
            return None; // caller's table is too small; TooMany, not truncation
        }
        out[n] = j;
        n += 1;
        j = skip_ws(b, skip_value(b, j)?);
        if j >= b.len() {
            return None;
        }
        match b[j] {
            b',' => j = skip_ws(b, j + 1),
            b']' => return Some(n),
            _ => return None,
        }
    }
}

/// The contents of the string value at `i`, as a raw byte range.
pub fn str_at(b: &[u8], i: usize) -> Option<(usize, usize)> {
    scan_string(b, i).map(|(s, e, _)| (s, e))
}

/// Follow `path` (a chain of object keys) from `i` and return the string value
/// found there. Missing members yield `None` rather than an error, so an
/// optional annotation reads the same as an absent one.
pub fn str_path<'a>(b: &'a [u8], i: usize, path: &[&[u8]]) -> Option<&'a [u8]> {
    let mut cur = i;
    for key in path {
        cur = obj_find(b, cur, key)?;
    }
    let (s, e) = str_at(b, cur)?;
    Some(&b[s..e])
}

// ---------------------------------------------------------------- JSON writer

/// A bounded JSON writer. Every method is a no-op once the buffer overflows, and
/// [`JsonW::finish`] reports it — so a truncated document can never be mistaken
/// for a complete one.
pub struct JsonW<'a> {
    buf: &'a mut [u8],
    pos: usize,
    overflow: bool,
}

impl<'a> JsonW<'a> {
    pub fn new(buf: &'a mut [u8]) -> Self {
        JsonW {
            buf,
            pos: 0,
            overflow: false,
        }
    }
    pub fn raw(&mut self, s: &[u8]) -> &mut Self {
        if !self.overflow {
            if self.pos + s.len() > self.buf.len() {
                self.overflow = true;
            } else {
                self.buf[self.pos..self.pos + s.len()].copy_from_slice(s);
                self.pos += s.len();
            }
        }
        self
    }
    /// A JSON string literal, escaping what RFC 8259 requires.
    pub fn str(&mut self, s: &[u8]) -> &mut Self {
        self.raw(b"\"");
        for &c in s {
            match c {
                b'"' => self.raw(b"\\\""),
                b'\\' => self.raw(b"\\\\"),
                b'\n' => self.raw(b"\\n"),
                b'\r' => self.raw(b"\\r"),
                b'\t' => self.raw(b"\\t"),
                0x00..=0x1f => {
                    let hexd = b"0123456789abcdef";
                    self.raw(b"\\u00");
                    self.raw(&[hexd[(c >> 4) as usize], hexd[(c & 0xf) as usize]])
                }
                _ => self.raw(&[c]),
            };
        }
        self.raw(b"\"")
    }
    pub fn num(&mut self, mut v: u64) -> &mut Self {
        let mut tmp = [0u8; 20];
        let mut n = 0;
        if v == 0 {
            tmp[0] = b'0';
            n = 1;
        } else {
            while v > 0 {
                tmp[n] = b'0' + (v % 10) as u8;
                v /= 10;
                n += 1;
            }
            tmp[..n].reverse();
        }
        self.raw(&tmp[..n])
    }
    /// `"key":` — the common prefix.
    pub fn key(&mut self, k: &[u8]) -> &mut Self {
        self.str(k).raw(b":")
    }
    /// A `sha256:<hex>` reference from a raw digest.
    pub fn digest_ref(&mut self, digest: &[u8; 32]) -> &mut Self {
        self.raw(b"\"sha256:");
        let hexd = b"0123456789abcdef";
        for b in digest {
            self.raw(&[hexd[(b >> 4) as usize], hexd[(b & 0x0f) as usize]]);
        }
        self.raw(b"\"")
    }
    pub fn finish(self) -> Result<usize, OciError> {
        if self.overflow {
            Err(OciError::TooLarge)
        } else {
            Ok(self.pos)
        }
    }
}

/// Parse a `sha256:<64 hex>` reference into a raw digest. Also accepts a bare
/// 64-hex string, which is how a blob key names itself.
pub fn parse_digest_ref(s: &[u8]) -> Option<[u8; 32]> {
    let hex = if let Some(rest) = s.strip_prefix(b"sha256:") {
        rest
    } else {
        s
    };
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    let mut i = 0;
    while i < 32 {
        let hi = hex_val(hex[i * 2])?;
        let lo = hex_val(hex[i * 2 + 1])?;
        out[i] = (hi << 4) | lo;
        i += 1;
    }
    Some(out)
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

// ------------------------------------------------------------------ the store

/// The scratch a push needs. In a no-alloc core every buffer is caller-owned;
/// grouping them keeps that explicit without turning the call into a wall of
/// arguments. All three must be distinct.
pub struct PushBufs<'a> {
    /// Receives the OCI manifest JSON.
    pub manifest: &'a mut [u8],
    /// Receives the rewritten `index.json`.
    pub index: &'a mut [u8],
    /// Holds the existing `index.json` while it is being rewritten.
    pub read: &'a mut [u8],
}

/// One layer to publish: its media type, its bytes, and (for artefacts) the
/// qualified name recorded as an annotation.
#[derive(Clone, Copy)]
pub struct Layer<'a> {
    pub media_type: &'a [u8],
    pub bytes: &'a [u8],
    pub name: &'a [u8],
}

/// Initialize an image layout: write `oci-layout` and, if absent, an empty
/// `index.json`. Idempotent — re-opening an existing store keeps its index.
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn oci_init(sys: &SyscallTable, scratch: &mut [u8]) -> Result<(), OciError> {
    put_named(sys, KEY_LAYOUT, LAYOUT_BODY)?;
    if get_named(sys, KEY_INDEX, scratch).is_err() {
        put_named(sys, KEY_INDEX, EMPTY_INDEX)?;
    }
    Ok(())
}

/// Push a bundle: store every layer as a blob, wrap them in an OCI manifest,
/// store that, and point `tag` at it in `index.json`. Returns the manifest
/// digest — the bundle reference.
///
/// A tag is a MUTABLE POINTER: re-pushing the same tag drops the old entry and
/// appends the new one, exactly as the host does, so tag mobility works and the
/// digest remains the only stable identity.
///
/// `manifest_buf` receives the manifest JSON and `index_buf` the rewritten
/// index; both are caller-owned so this core allocates nothing.
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn oci_push(
    sys: &SyscallTable,
    layers: &[Layer],
    tag: &[u8],
    provenance: &[u8],
    source_rev: &[u8],
    bufs: PushBufs,
) -> Result<[u8; 32], OciError> {
    let PushBufs {
        manifest: manifest_buf,
        index: index_buf,
        read: read_buf,
    } = bufs;
    if layers.len() > MAX_LAYERS {
        return Err(OciError::TooMany);
    }
    // 1. Every layer becomes a blob under its own digest.
    let mut digests = [[0u8; 32]; MAX_LAYERS];
    for (i, l) in layers.iter().enumerate() {
        digests[i] = blob_put(sys, l.bytes)?;
    }
    // 2. The config is the empty blob, per the OCI artefact guidance.
    let empty_digest = blob_put(sys, b"{}")?;

    // 3. The manifest.
    let mut w = JsonW::new(manifest_buf);
    w.raw(b"{").key(b"schemaVersion").num(2).raw(b",");
    w.key(b"mediaType").str(MEDIA_TYPE_MANIFEST).raw(b",");
    w.key(b"config").raw(b"{");
    w.key(b"mediaType").str(MEDIA_TYPE_EMPTY).raw(b",");
    w.key(b"digest").digest_ref(&empty_digest).raw(b",");
    w.key(b"size").num(2).raw(b"},");
    w.key(b"layers").raw(b"[");
    for (i, l) in layers.iter().enumerate() {
        if i > 0 {
            w.raw(b",");
        }
        w.raw(b"{").key(b"mediaType").str(l.media_type).raw(b",");
        w.key(b"digest").digest_ref(&digests[i]).raw(b",");
        w.key(b"size").num(l.bytes.len() as u64);
        if !l.name.is_empty() {
            w.raw(b",").key(b"annotations").raw(b"{");
            w.key(ANNOTATION_NAME).str(l.name).raw(b"}");
        }
        w.raw(b"}");
    }
    w.raw(b"],").key(b"annotations").raw(b"{");
    w.key(ANNOTATION_PROVENANCE).str(provenance).raw(b",");
    w.key(ANNOTATION_SOURCE_REV).str(source_rev).raw(b",");
    w.key(ANNOTATION_NAME).str(tag).raw(b"}}");
    let mlen = w.finish()?;
    let manifest_digest = blob_put(sys, &manifest_buf[..mlen])?;

    // 4. Rewrite index.json: drop any entry already holding this tag, then
    //    append the new one. Digests are truth; tags move.
    let ilen = get_named(sys, KEY_INDEX, read_buf)?;
    let idx = &read_buf[..ilen];
    let manifests_pos = obj_find(idx, 0, b"manifests").ok_or(OciError::Malformed)?;
    let mut elems = [0usize; MAX_INDEX_ENTRIES];
    let n = arr_elems(idx, manifests_pos, &mut elems).ok_or(OciError::TooMany)?;

    let mut w = JsonW::new(index_buf);
    w.raw(b"{").key(b"schemaVersion").num(2).raw(b",");
    w.key(b"manifests").raw(b"[");
    let mut written = 0usize;
    for &e in elems.iter().take(n) {
        // Keep every entry whose ref.name differs from the tag being moved.
        if str_path(idx, e, &[b"annotations", ANNOTATION_NAME]) == Some(tag) {
            continue;
        }
        let end = skip_value(idx, e).ok_or(OciError::Malformed)?;
        if written > 0 {
            w.raw(b",");
        }
        w.raw(&idx[e..end]);
        written += 1;
    }
    if written > 0 {
        w.raw(b",");
    }
    w.raw(b"{")
        .key(b"mediaType")
        .str(MEDIA_TYPE_MANIFEST)
        .raw(b",");
    w.key(b"digest").digest_ref(&manifest_digest).raw(b",");
    w.key(b"size").num(mlen as u64).raw(b",");
    w.key(b"annotations")
        .raw(b"{")
        .key(ANNOTATION_NAME)
        .str(tag);
    w.raw(b"}}]}");
    let nlen = w.finish()?;
    put_named(sys, KEY_INDEX, &index_buf[..nlen])?;

    Ok(manifest_digest)
}

/// Resolve `tag` to its bundle digest via `index.json`.
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn oci_resolve_tag(
    sys: &SyscallTable,
    tag: &[u8],
    read_buf: &mut [u8],
) -> Result<[u8; 32], OciError> {
    let ilen = get_named(sys, KEY_INDEX, read_buf)?;
    let idx = &read_buf[..ilen];
    let manifests_pos = obj_find(idx, 0, b"manifests").ok_or(OciError::Malformed)?;
    let mut elems = [0usize; MAX_INDEX_ENTRIES];
    let n = arr_elems(idx, manifests_pos, &mut elems).ok_or(OciError::TooMany)?;
    for &e in elems.iter().take(n) {
        if str_path(idx, e, &[b"annotations", ANNOTATION_NAME]) == Some(tag) {
            let d = str_path(idx, e, &[b"digest"]).ok_or(OciError::Malformed)?;
            return parse_digest_ref(d).ok_or(OciError::Malformed);
        }
    }
    Err(OciError::NotFound)
}

/// Fetch a bundle by manifest digest, handing each layer to `on_layer` as
/// `(media_type, name, bytes)`.
///
/// Every layer is read through `blob_get`, so a corrupted or substituted blob
/// fails the content-address check here rather than being handed on — fetch
/// VERIFIES, it does not merely read.
///
/// # Safety
/// `sys` must be the module's live syscall table.
pub unsafe fn oci_fetch<F>(
    sys: &SyscallTable,
    manifest_digest: &[u8; 32],
    manifest_buf: &mut [u8],
    layer_buf: &mut [u8],
    mut on_layer: F,
) -> Result<usize, OciError>
where
    F: FnMut(&[u8], &[u8], &[u8]),
{
    let mlen = blob_get(sys, manifest_digest, manifest_buf)?;
    // Copy the layer descriptors out before `manifest_buf` is reused: offsets
    // into it stop being valid the moment a layer is read.
    let mut descs = [([0u8; 32], 0usize, 0usize, 0usize, 0usize); MAX_LAYERS];
    let ndesc;
    {
        let man = &manifest_buf[..mlen];
        let layers_pos = obj_find(man, 0, b"layers").ok_or(OciError::Malformed)?;
        let mut elems = [0usize; MAX_LAYERS];
        let n = arr_elems(man, layers_pos, &mut elems).ok_or(OciError::TooMany)?;
        for (i, &e) in elems.iter().take(n).enumerate() {
            let d = str_path(man, e, &[b"digest"]).ok_or(OciError::Malformed)?;
            let digest = parse_digest_ref(d).ok_or(OciError::Malformed)?;
            let mt = obj_find(man, e, b"mediaType")
                .and_then(|p| str_at(man, p))
                .ok_or(OciError::Malformed)?;
            let nm = obj_find(man, e, b"annotations")
                .and_then(|p| obj_find(man, p, ANNOTATION_NAME))
                .and_then(|p| str_at(man, p))
                .unwrap_or((0, 0));
            descs[i] = (digest, mt.0, mt.1, nm.0, nm.1);
        }
        ndesc = n;
    }

    let mut media = [0u8; 64];
    let mut name = [0u8; 128];
    for &(digest, ms, me, ns, ne) in descs.iter().take(ndesc) {
        let mlen2 = me - ms;
        let nlen2 = ne - ns;
        if mlen2 > media.len() || nlen2 > name.len() {
            return Err(OciError::TooLarge);
        }
        media[..mlen2].copy_from_slice(&manifest_buf[ms..me]);
        name[..nlen2].copy_from_slice(&manifest_buf[ns..ne]);
        let blen = blob_get(sys, &digest, layer_buf)?; // verifies the address
        on_layer(&media[..mlen2], &name[..nlen2], &layer_buf[..blen]);
    }
    Ok(ndesc)
}
