// Bounded, no_std, no-alloc BYTE-DESERIALIZATION VM — the mirror of ser_core.rs.
// `include!`d by both the host harness and the module. Turns framing a single
// message — a length-prefixed reply, a delimited line, a protobuf field — into
// ordinary bytecode: read opcodes advance a cursor over an input buffer and push
// values; the message-construction opcodes (SET_FIELD / FINISH_MSG from
// vm_core.rs) assemble a record. It frames messages; it does not speak
// protocols — handshakes and reply-dependent state are out of scope (see
// docs/guides/wire-codec.md).

/// Byte-read opcodes (0x70+, disjoint from the value/message/serialize opcodes).
pub mod rd {
    pub const SKIP: u8 = 0x70; // n:u16 LE — advance the cursor
    pub const LIT: u8 = 0x71; // len:u16 LE, bytes — expect these bytes (else error)
    pub const UNTIL: u8 = 0x72; // delim:u8 — push Bytes up to delim, then skip it
    pub const TAKE: u8 = 0x73; // n:u16 LE — push the next n bytes
    pub const TAKEN: u8 = 0x74; // pop Int(n) → push the next n bytes
    pub const INT: u8 = 0x75; // width:u8, endian:u8 — read a binary int → push Int
    pub const DECINT: u8 = 0x76; // read ASCII decimal digits → push Int
    pub const SEEK: u8 = 0x77; // len:u16 LE, bytes — advance PAST the first occurrence of the sequence (e.g. skip HTTP headers to "\r\n\r\n")
    pub const REST: u8 = 0x78; // push all remaining bytes from the cursor to the end
    pub const H2MSG: u8 = 0x79; // walk HTTP/2 frames from the cursor to the first DATA frame, push its gRPC Length-Prefixed-Message payload (the response protobuf)
    /// `field:u32 LE` — pop a protobuf message (Bytes), push the value of the
    /// FIRST occurrence of that field number (len-delimited → Bytes,
    /// varint/fixed → Int, absent → Null). The scan stops at that occurrence;
    /// a message that does not frame before it (a group, a length past its
    /// end, an over-long varint) is `Truncated`.
    pub const PBFIELD: u8 = 0x7A;
    /// `delim:u8` — like [`UNTIL`], but TOLERANT of exhaustion.
    ///
    /// `UNTIL` fails with `Truncated` when the delimiter is not there, so a
    /// program that reads N delimited fields only works on input with exactly
    /// N of them. Splitting a VARIABLE-ARITY sequence — a URL path, a CSV row,
    /// a header block, a SIP request line — needs a reader that can run out;
    /// this is that reader.
    ///
    /// Three cases, none an error:
    ///   * delimiter found — push the bytes before it and consume it (as `UNTIL`)
    ///   * input remains but no delimiter — push the REMAINDER, cursor to the end
    ///   * cursor already at the end — push EMPTY
    ///
    /// So `UNTIL_OPT` repeated N times reads up to N fields and pads the rest
    /// with empty, which is exactly what a flat record wants: field numbers stay
    /// stable and a consumer distinguishes "absent" by emptiness rather than by
    /// the program having failed.
    pub const UNTIL_OPT: u8 = 0x7B;

    // ── value readers: pop a Bytes value, push a part of it ─────────────────
    //
    // The cursor ops above read the INPUT; these read a value already pushed —
    // a request target, a header block, a JSON body — so a program can take a
    // section of the input whole (`TAKEN`) and then look inside it without the
    // reader running past its end. None fails on absence: an absent part is
    // EMPTY, which is what a flat record's consumer reads as "not there".
    /// `len:u16 LE, bytes` — the part BEFORE the first occurrence of the
    /// sequence; the whole value when it does not occur (`/path?q` → `/path`).
    pub const BEFORE: u8 = 0x7C;
    /// `len:u16 LE, bytes` — the part AFTER the first occurrence; EMPTY when it
    /// does not occur (`/path?q` → `q`; `Bearer t` after `Bearer ` → `t`).
    pub const AFTER: u8 = 0x7D;
    /// `delim:u8, index:u8` — the index-th `delim`-separated part (0-based,
    /// empty parts counted); EMPTY past the last.
    pub const PART: u8 = 0x7E;
    /// `len:u16 LE, name` — an HTTP header block's value for `name` (field
    /// names compared ASCII-case-insensitively, the value trimmed); EMPTY when
    /// absent.
    pub const HDR: u8 = 0x7F;
    /// Pop b, pop a — push a when it is non-empty, else b (a path's name, else
    /// the body's).
    pub const COALESCE: u8 = 0x81;
    /// `n:u8` — pop n values (bytes, text or an integer as decimal), push their
    /// concatenation, first-pushed first. Built in the caller's SCRATCH (see
    /// [`eval_decode_scratch`]); `ScratchOverflow` when it does not fit.
    pub const CAT: u8 = 0x82;
    /// Pop a value, push its bytes as lowercase hex (in scratch). An integer
    /// is hexed as its decimal text, the text `CAT` gives it.
    pub const HEX: u8 = 0x83;
    /// `n:u16 LE` — pop a value, push it without its first n bytes; EMPTY when
    /// shorter.
    pub const DROP: u8 = 0x84;
    /// Pop cond, pop value — push value when cond is non-empty, else EMPTY (a
    /// derived value that must vanish with the thing it was derived from).
    pub const GATE: u8 = 0x85;

    // ── record and document readers with operands from the stack ───────────
    /// `field:u8` — pop a Chronicle record frame (bytes), push that field's
    /// value (a bytes field → Bytes, an integer → Int, a message → Frame);
    /// ABSENT (Null) when the frame does not carry it. The frame sibling of
    /// [`PBFIELD`], so a decode program can take a typed record in as a
    /// protocol unit — a connector's reply, a stage's output.
    pub const FIELD: u8 = 0x86;
    /// Pop a path, pop a document — push the value at that dotted path (object
    /// keys; decimal array indices): a string's bytes between its quotes,
    /// escapes left as written (not unescaped), a number/bool/null as written,
    /// an object or array as its bytes. ABSENT (Null) when the path is not
    /// there, so a present empty string and a missing member stay distinct.
    /// The reader locates; it does not validate the document. The path is
    /// DATA: a policy read at run time names what to look for.
    pub const JSONAT: u8 = 0x87;
    /// Pop raw JSON, pop a path, pop a document — push the document with
    /// `"key":raw` inserted where the path has nothing: at the top level
    /// (`k`) or one object down (`parent.k`, the parent an object that
    /// exists). A present path, any deeper path, a key JSON would need
    /// escaped, or an empty raw leaves the document as it is. Built in the
    /// caller's SCRATCH; `ScratchOverflow` when it does not fit.
    pub const JSONDEF: u8 = 0x88;
    /// Pop a value, push it URL-decoded as a query string is (`%XX` → the
    /// byte, `+` → space); a `%` not followed by two hex digits stays as
    /// written. Built in scratch.
    pub const PCTDEC: u8 = 0x89;
    /// `delim:u8, open:u8, close:u8, index:u8` — as [`PART`], but a delimiter
    /// between `open` and `close` does not split. Distinct brackets nest (the
    /// parts of `a=b,c in (x,y)` on `,` with `(` `)` are `a=b` and
    /// `c in (x,y)`) and an unbalanced `close` counts as depth 0; equal ones
    /// toggle, as quotes do (`a,"b,c",d` with `"` `"` gives `"b,c"`, quotes
    /// kept). A `delim` equal to `open` or `close` is a bracket, never a split.
    pub const PARTB: u8 = 0x8B;
    /// Pop a descriptor, pop a protobuf message — push the message as JSON,
    /// built in scratch, read by the descriptor (see [`pbk`]). Null when the
    /// message does not decode or the descriptor is malformed;
    /// `ScratchOverflow` — never Null, never a truncated document — when the
    /// JSON does not fit or nesting passes [`PBJ_MAX_FRAMES`]. Its work counts
    /// against the program's cost budget, and stops once that is spent
    /// (`CostExceeded`).
    pub const PBJSON: u8 = 0x8C;
}

/// The descriptor [`rd::PBJSON`] reads a protobuf message by: a table of message
/// descriptors, `[n_msgs:u8]` then per message `[n_fields:u8]` and per field
/// `[field:u8][kind:u8][arg:u8][name_len:u8][name]`. Message 0 describes the
/// message itself; `arg` names another message by its index. A field the
/// descriptor does not list is skipped; a field the message does not carry is
/// left out. Fields are written in descriptor order.
///
/// A kind byte is a kind in its low five bits, plus the flags below. The
/// kinds are proto3's JSON mapping, with `int64` written as a number:
pub mod pbk {
    /// A string, JSON-escaped; one that is not UTF-8 does not decode.
    pub const STR: u8 = 1;
    /// A varint as a signed 64-bit integer.
    pub const INT: u8 = 2;
    /// A varint as an unsigned 64-bit integer.
    pub const UINT: u8 = 3;
    /// A zigzag varint (`sint32`/`sint64`).
    pub const SINT: u8 = 4;
    pub const BOOL: u8 = 5;
    /// Bytes, as standard base64 with padding.
    pub const BYTES: u8 = 6;
    /// A nested message, described by descriptor `arg`.
    pub const MSG: u8 = 7;
    /// A map: each occurrence is an entry message `{1 key, 2 value}` described
    /// by descriptor `arg`, written as one JSON object keyed by the entry's key.
    pub const MAP: u8 = 8;
    /// `{1 seconds, 2 nanos}` as RFC 3339 UTC (`google.protobuf.Timestamp`);
    /// fractional seconds only when `nanos` is not zero. The zero time is left
    /// out, as an unset one is. A time outside `0001-01-01T00:00:00Z` to
    /// `9999-12-31T23:59:59.999999999Z`, or `nanos` outside `0..=999_999_999`,
    /// does not decode.
    pub const TIMESTAMP: u8 = 9;
    /// A message written as the value of the first field of descriptor `arg`,
    /// bare (a wrapper type); an empty wrapper is that field's default (`0`,
    /// `false`, `""`), as proto3 JSON writes it.
    pub const UNWRAP: u8 = 10;
    /// A tagged union: descriptor `arg`'s first field is the discriminator, and
    /// its value `d` selects the field listed `d + 1`, written bare.
    pub const UNION: u8 = 11;
    /// The kind bits.
    pub const KIND: u8 = 0x1F;
    /// Every occurrence, as a JSON array (packed or not, for numbers).
    pub const REPEATED: u8 = 0x40;
    /// Leave the field out when it holds its type's default — empty, zero or
    /// false — as JSON written with `omitempty` does.
    pub const OMIT_DEFAULT: u8 = 0x80;
}

// ---- JSON: a bounded path reader over a document's bytes -------------------
//
// Read by `rd::JSONAT` and `rd::JSONDEF`. It reads where the document
// lives and allocates nothing. Every step moves forward through the
// bytes — a member or element that is not followed by `,` or its closer ends the
// read as absent — so the work is bounded by the document's length, whatever
// index or path a caller names. A value the document ends inside of (an open
// string or container) is absent, never read as a shorter value. Keys are
// compared as written: a key the document spells with an escape is matched only
// by a path spelling it the same way.

/// Skip JSON whitespace from `i`.
fn json_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\n' | b'\r') {
        i += 1;
    }
    i
}

/// The end (one past the closing quote) of the JSON string whose opening
/// quote is at `i`; `None` when the document ends before the string does.
fn json_skip_str(b: &[u8], i: usize) -> Option<usize> {
    let mut j = i + 1;
    while j < b.len() {
        match b[j] {
            b'"' => return Some(j + 1),
            b'\\' => j += 2,
            _ => j += 1,
        }
    }
    None
}

/// The end of the JSON value starting at `i`, bounded by the bytes. `None`
/// when a string or container is still open at the end of the document: a
/// value cut short is absent, never a shorter value. A container is walked by
/// counting its brackets, not by recursing, so nesting costs no stack.
fn json_skip(b: &[u8], i: usize) -> Option<usize> {
    let i = json_ws(b, i);
    if i >= b.len() {
        return Some(i);
    }
    match b[i] {
        b'"' => json_skip_str(b, i),
        b'{' | b'[' => {
            let mut depth = 0usize;
            let mut j = i;
            while j < b.len() {
                match b[j] {
                    b'"' => {
                        j = json_skip_str(b, j)?;
                        continue;
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        // The walk opened on a bracket, so `depth` is at
                        // least 1 at every closer it meets.
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
            let mut j = i;
            while j < b.len() && !matches!(b[j], b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r')
            {
                j += 1;
            }
            Some(j)
        }
    }
}

/// The value at one step (`key` of an object, or index of an array) from the
/// value starting at `i`: its start. `None` when the step is absent or the
/// container is malformed on the way to it.
fn json_step(b: &[u8], i: usize, seg: &[u8]) -> Option<usize> {
    let i = json_ws(b, i);
    match *b.get(i)? {
        b'{' => {
            let mut j = json_ws(b, i + 1);
            loop {
                // `"key": value`, then `,` or the end of the object.
                if b.get(j) != Some(&b'"') {
                    return None;
                }
                let ke = json_skip(b, j)?;
                let key = b.get(j + 1..ke.checked_sub(1)?)?;
                j = json_ws(b, ke);
                if b.get(j) != Some(&b':') {
                    return None;
                }
                let vs = json_ws(b, j + 1);
                if key == seg {
                    return Some(vs);
                }
                j = json_ws(b, json_skip(b, vs)?);
                match b.get(j) {
                    Some(b',') => j = json_ws(b, j + 1),
                    _ => return None,
                }
            }
        }
        b'[' => {
            let mut want = 0usize;
            for &c in seg {
                if !c.is_ascii_digit() {
                    return None;
                }
                want = want.checked_mul(10)?.checked_add((c - b'0') as usize)?;
            }
            let mut j = json_ws(b, i + 1);
            let mut k = 0usize;
            loop {
                // An element must be there and must move the cursor; then `,`
                // or the end of the array. An index past the last element is
                // therefore reached only by walking the array, never by counting
                // in place.
                let e = json_skip(b, j)?;
                if j >= b.len() || e == j {
                    return None;
                }
                if k == want {
                    return Some(j);
                }
                j = json_ws(b, e);
                match b.get(j) {
                    Some(b',') => j = json_ws(b, j + 1),
                    _ => return None,
                }
                k += 1;
            }
        }
        _ => None,
    }
}

/// The raw span `[start, end)` of the value at a dotted path (object keys;
/// decimal array indices), or `None` when absent — including a value the
/// document ends inside of. Empty segments are skipped, so an empty path names
/// the document itself.
fn json_locate(doc: &[u8], path: &[u8]) -> Option<(usize, usize)> {
    let mut i = 0usize;
    for seg in path.split(|&c| c == b'.') {
        if seg.is_empty() {
            continue;
        }
        i = json_step(doc, i, seg)?;
    }
    let i = json_ws(doc, i);
    if i >= doc.len() {
        return None;
    }
    Some((i, json_skip(doc, i)?))
}

/// A span as a value reads: a string's content without its quotes (escape
/// sequences left as written), anything else as written.
fn json_unquote(doc: &[u8], (i, e): (usize, usize)) -> (usize, usize) {
    if e >= i + 2 && doc[i] == b'"' {
        (i + 1, e - 1)
    } else {
        (i, e)
    }
}

/// Where a default for `path` goes in `doc`: the index of the enclosing
/// object's `{`, the key's range within `path`, and whether that object is
/// empty. `None` leaves the document as it is — the document ends inside a
/// string or container (a cut-short document may hold the key past its end),
/// the path is present (a present field, whatever its value, is the
/// caller's), it is neither `k` (top level)
/// nor `parent.k` (one level down, the parent an object that exists), or its
/// key is one JSON would need escaped (written verbatim, it would come out
/// malformed or smuggle in members). Used by `rd::JSONDEF`.
fn json_default_at(doc: &[u8], path: &[u8]) -> Option<(usize, core::ops::Range<usize>, bool)> {
    json_skip(doc, 0)?;
    if json_locate(doc, path).is_some() {
        return None;
    }
    let (parent, key) = match path.iter().position(|&c| c == b'.') {
        None => (None, 0..path.len()),
        Some(d) if path.iter().rposition(|&c| c == b'.') == Some(d) => {
            (Some(0..d), d + 1..path.len())
        }
        Some(_) => return None,
    };
    if path
        .get(key.clone())?
        .iter()
        .any(|&c| c == b'"' || c == b'\\' || c < 0x20)
    {
        return None;
    }
    let obj_at = match parent {
        None => json_ws(doc, 0),
        Some(pp) => json_locate(doc, path.get(pp)?)?.0,
    };
    if doc.get(obj_at) != Some(&b'{') {
        return None;
    }
    let empty = doc.get(json_ws(doc, obj_at + 1)) == Some(&b'}');
    Some((obj_at, key, empty))
}

/// Message descriptors a [`rd::PBJSON`] table may hold.
pub const PBJ_MAX_MSGS: usize = 32;
/// Objects, arrays and maps open at once while writing: bounds nesting.
pub const PBJ_MAX_FRAMES: usize = 16;

/// A protobuf field's wire value.
#[derive(Clone, Copy)]
enum Pv<'m> {
    V(u64),
    B(&'m [u8]),
    F(u64),
}

/// The field at `pos` in `m`: `(number, value, next)`; `Ok(None)` at the end,
/// `Err` when the message does not frame (a group, a length past its end).
fn pb_next(m: &[u8], pos: usize) -> Result<Option<(u32, Pv<'_>, usize)>, ()> {
    if pos >= m.len() {
        return Ok(None);
    }
    let (tag, p) = pb_varint(m, pos).ok_or(())?;
    let field = u32::try_from(tag >> 3).map_err(|_| ())?;
    let (v, next) = match tag & 7 {
        0 => {
            let (v, n) = pb_varint(m, p).ok_or(())?;
            (Pv::V(v), n)
        }
        1 => {
            let b = m.get(p..p.checked_add(8).ok_or(())?).ok_or(())?;
            (
                Pv::F(u64::from_le_bytes(b.try_into().map_err(|_| ())?)),
                p + 8,
            )
        }
        2 => {
            let (len, n) = pb_varint(m, p).ok_or(())?;
            let end = usize::try_from(len)
                .ok()
                .and_then(|l| n.checked_add(l))
                .ok_or(())?;
            (Pv::B(m.get(n..end).ok_or(())?), end)
        }
        5 => {
            let b = m.get(p..p.checked_add(4).ok_or(())?).ok_or(())?;
            (
                Pv::F(u32::from_le_bytes(b.try_into().map_err(|_| ())?) as u64),
                p + 4,
            )
        }
        _ => return Err(()),
    };
    Ok(Some((field, v, next)))
}

/// The next occurrence of `field` in `m` at or after `pos`: `(value, next)`.
fn pb_find<'m>(
    m: &'m [u8],
    mut pos: usize,
    field: u32,
    work: &mut u64,
) -> Result<Option<(Pv<'m>, usize)>, ()> {
    while let Some((f, v, next)) = pb_next(m, pos)? {
        *work += 1;
        pos = next;
        if f == field {
            return Ok(Some((v, next)));
        }
    }
    Ok(None)
}

/// The last occurrence of `field` in `m` (proto: the last one wins).
fn pb_last<'m>(m: &'m [u8], field: u32, work: &mut u64) -> Result<Option<Pv<'m>>, ()> {
    let (mut pos, mut last) = (0usize, None);
    while let Some((v, next)) = pb_find(m, pos, field, work)? {
        last = Some(v);
        pos = next;
    }
    Ok(last)
}

/// A validated descriptor table, with each message's first entry located.
struct PbDesc<'d> {
    t: &'d [u8],
    start: [u16; PBJ_MAX_MSGS],
    n: usize,
}

/// One descriptor entry.
#[derive(Clone, Copy)]
struct PbEntry<'d> {
    field: u32,
    kind: u8,
    arg: u8,
    name: &'d [u8],
}

impl<'d> PbDesc<'d> {
    fn parse(t: &'d [u8]) -> Option<Self> {
        let n = *t.first()? as usize;
        if n == 0 || n > PBJ_MAX_MSGS || t.len() > u16::MAX as usize {
            return None;
        }
        let mut d = PbDesc {
            t,
            start: [0; PBJ_MAX_MSGS],
            n,
        };
        let mut p = 1usize;
        for i in 0..n {
            d.start[i] = p as u16;
            let nf = *t.get(p)? as usize;
            p += 1;
            for _ in 0..nf {
                let h = t.get(p..p + 4)?;
                let (kind, arg) = (h[1], h[2] as usize);
                let k = kind & pbk::KIND;
                if !(pbk::STR..=pbk::UNION).contains(&k) {
                    return None;
                }
                let refers = matches!(k, pbk::MSG | pbk::MAP | pbk::UNWRAP | pbk::UNION);
                if refers && arg >= n {
                    return None;
                }
                p += 4 + h[3] as usize;
                if p > t.len() {
                    return None;
                }
            }
        }
        if p != t.len() {
            return None;
        }
        Some(d)
    }

    /// Message `i`'s entries: `(count, offset of the first)`. A message the
    /// table does not hold has none, rather than reading another's offsets.
    fn entries(&self, i: u8) -> (u8, u16) {
        if i as usize >= self.n {
            return (0, 0);
        }
        let s = self.start[i as usize] as usize;
        (self.t.get(s).copied().unwrap_or(0), (s + 1) as u16)
    }

    /// The entry at `at`, and the offset after it. The table was validated.
    fn entry(&self, at: u16) -> (PbEntry<'d>, u16) {
        let a = at as usize;
        let h = self.t.get(a..a + 4).unwrap_or(&[0, 0, 0, 0]);
        let nl = h[3] as usize;
        let e = PbEntry {
            field: h[0] as u32,
            kind: h[1],
            arg: h[2],
            name: self.t.get(a + 4..a + 4 + nl).unwrap_or(&[]),
        };
        (e, (a + 4 + nl) as u16)
    }

    /// Message `i`'s `k`-th entry.
    fn nth(&self, i: u8, k: u8) -> Option<PbEntry<'d>> {
        let (count, mut at) = self.entries(i);
        if k >= count {
            return None;
        }
        for _ in 0..k {
            at = self.entry(at).1;
        }
        Some(self.entry(at).0)
    }
}

/// JSON being written into scratch; `over` once it no longer fits.
struct JsonOut<'b> {
    buf: &'b mut [u8],
    n: usize,
    over: bool,
}

impl JsonOut<'_> {
    fn put1(&mut self, c: u8) {
        match self.buf.get_mut(self.n) {
            Some(d) => {
                *d = c;
                self.n += 1;
            }
            None => self.over = true,
        }
    }
    fn put(&mut self, s: &[u8]) {
        for &c in s {
            self.put1(c);
        }
    }
    fn dec(&mut self, mut v: u64, neg: bool) {
        if neg {
            self.put1(b'-');
        }
        let mut d = [0u8; 20];
        let mut i = d.len();
        loop {
            i -= 1;
            d[i] = b'0' + (v % 10) as u8;
            v /= 10;
            if v == 0 {
                break;
            }
        }
        self.put(&d[i..]);
    }
    fn int(&mut self, v: i64) {
        self.dec(v.unsigned_abs(), v < 0);
    }
    fn digits(&mut self, v: u64, width: usize) {
        let mut d = [b'0'; 10];
        let mut x = v;
        for i in (0..width.min(10)).rev() {
            d[i] = b'0' + (x % 10) as u8;
            x /= 10;
        }
        self.put(&d[..width.min(10)]);
    }
    /// A JSON string: `"` and `\` escaped, newline, return and tab in short
    /// form, other control bytes as `\u00XX` (as Go writes them); other bytes
    /// as they are.
    fn string(&mut self, s: &[u8]) {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        self.put1(b'"');
        for &c in s {
            match c {
                b'"' | b'\\' => {
                    self.put1(b'\\');
                    self.put1(c);
                }
                b'\n' => self.put(b"\\n"),
                b'\r' => self.put(b"\\r"),
                b'\t' => self.put(b"\\t"),
                0..=0x1f => {
                    self.put(b"\\u00");
                    self.put1(HEX[(c >> 4) as usize]);
                    self.put1(HEX[(c & 15) as usize]);
                }
                _ => self.put1(c),
            }
        }
        self.put1(b'"');
    }
    fn base64(&mut self, s: &[u8]) {
        const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        self.put1(b'"');
        for c in s.chunks(3) {
            let b = [
                c[0],
                c.get(1).copied().unwrap_or(0),
                c.get(2).copied().unwrap_or(0),
            ];
            let v = (b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32;
            for k in 0..4 {
                if k <= c.len() {
                    self.put1(A[((v >> (18 - 6 * k)) & 63) as usize]);
                } else {
                    self.put1(b'=');
                }
            }
        }
        self.put1(b'"');
    }
    /// RFC 3339 UTC from Unix seconds and nanoseconds, as [`pb_timestamp`]
    /// bounds them: years 0001 to 9999, `nanos` below one second.
    fn timestamp(&mut self, secs: i64, nanos: u32) {
        // Days to a civil date (Howard Hinnant's algorithm), in whole-day
        // arithmetic that stays inside i64 for any i64 second count.
        let days = secs.div_euclid(86_400);
        let sod = secs.rem_euclid(86_400) as u64;
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = (doy - (153 * mp + 2) / 5 + 1) as u64;
        let month = (if mp < 10 { mp + 3 } else { mp - 9 }) as u64;
        let year = yoe + era * 400 + i64::from(month <= 2);
        self.put1(b'"');
        self.digits(year.unsigned_abs(), 4);
        self.put1(b'-');
        self.digits(month, 2);
        self.put1(b'-');
        self.digits(day, 2);
        self.put1(b'T');
        self.digits(sod / 3600, 2);
        self.put1(b':');
        self.digits(sod / 60 % 60, 2);
        self.put1(b':');
        self.digits(sod % 60, 2);
        if nanos != 0 {
            let mut n = nanos as u64;
            let mut w = 9;
            while n.is_multiple_of(10) {
                n /= 10;
                w -= 1;
            }
            self.put1(b'.');
            self.digits(n, w);
        }
        self.put(b"Z\"");
    }
}

/// `{1 seconds, 2 nanos}`; `None` when it does not decode or lies outside
/// `google.protobuf.Timestamp`'s range (0001-01-01 to 9999-12-31, `nanos` in
/// `0..=999_999_999`) — refused, never clamped into a different time.
fn pb_timestamp(m: &[u8], work: &mut u64) -> Option<(i64, u32)> {
    let secs = match pb_last(m, 1, work).ok()? {
        Some(Pv::V(v)) => v as i64,
        None => 0,
        _ => return None,
    };
    let nanos = match pb_last(m, 2, work).ok()? {
        Some(Pv::V(v)) => u32::try_from(v).ok().filter(|&n| n <= 999_999_999)?,
        None => 0,
        _ => return None,
    };
    if !(-62_135_596_800..=253_402_300_799).contains(&secs) {
        return None;
    }
    Some((secs, nanos))
}

/// Whether `v` is its kind's default (empty, zero, false, the zero time).
fn pb_default(kind: u8, v: Pv<'_>, work: &mut u64) -> bool {
    match (kind, v) {
        (pbk::TIMESTAMP, Pv::B(m)) => pb_timestamp(m, work) == Some((0, 0)),
        (_, Pv::B(b)) => b.is_empty(),
        (_, Pv::V(x) | Pv::F(x)) => x == 0,
    }
}

/// Follow `UNWRAP`/`UNION` to the value written in their place: `Ok(None)`
/// when that value is absent.
fn pb_resolve<'m>(
    d: &PbDesc<'_>,
    mut kind: u8,
    mut arg: u8,
    mut v: Pv<'m>,
    work: &mut u64,
) -> Result<Option<(u8, u8, Pv<'m>)>, ()> {
    for _ in 0..PBJ_MAX_FRAMES {
        let k = kind & pbk::KIND;
        if k != pbk::UNWRAP && k != pbk::UNION {
            return Ok(Some((k, arg, v)));
        }
        let Pv::B(m) = v else { return Err(()) };
        let pick = if k == pbk::UNWRAP {
            0
        } else {
            let disc = d.nth(arg, 0).ok_or(())?;
            match pb_last(m, disc.field, work)? {
                Some(Pv::V(x)) => u8::try_from(x)
                    .ok()
                    .and_then(|x| x.checked_add(1))
                    .ok_or(())?,
                None => 1,
                _ => return Err(()),
            }
        };
        let e = d.nth(arg, pick).ok_or(())?;
        match pb_last(m, e.field, work)? {
            Some(inner) => {
                kind = e.kind;
                arg = e.arg;
                v = inner;
            }
            // An empty wrapper holds its value's default: proto3 JSON writes
            // `Int64Value{}` as `0`, not as absent.
            None if k == pbk::UNWRAP => {
                kind = e.kind;
                arg = e.arg;
                v = match e.kind & pbk::KIND {
                    pbk::INT | pbk::UINT | pbk::SINT | pbk::BOOL => Pv::V(0),
                    _ => Pv::B(&[]),
                };
            }
            None => return Ok(None),
        }
    }
    Err(())
}

/// Whether `b` is well-formed UTF-8: no overlong forms, surrogates or code
/// points past U+10FFFF. Local, so the module links no `core::str` path.
fn pb_utf8(b: &[u8]) -> bool {
    let mut i = 0usize;
    while let Some(&c) = b.get(i) {
        // Continuations after the lead, and the first one's range.
        let (n, lo, hi) = match c {
            0x00..=0x7F => {
                i += 1;
                continue;
            }
            0xC2..=0xDF => (1, 0x80, 0xBF),
            0xE0 => (2, 0xA0, 0xBF),
            0xE1..=0xEC | 0xEE..=0xEF => (2, 0x80, 0xBF),
            0xED => (2, 0x80, 0x9F),
            0xF0 => (3, 0x90, 0xBF),
            0xF1..=0xF3 => (3, 0x80, 0xBF),
            0xF4 => (3, 0x80, 0x8F),
            _ => return false,
        };
        if !matches!(b.get(i + 1), Some(&x) if lo <= x && x <= hi) {
            return false;
        }
        for k in 2..=n {
            if !matches!(b.get(i + k), Some(&x) if x & 0xC0 == 0x80) {
                return false;
            }
        }
        i += n + 1;
    }
    true
}

/// Write a resolved scalar (not a message, map or list).
fn pb_scalar(o: &mut JsonOut<'_>, kind: u8, v: Pv<'_>, work: &mut u64) -> Result<(), ()> {
    match (kind, v) {
        (pbk::STR, Pv::B(b)) => {
            if !pb_utf8(b) {
                return Err(());
            }
            o.string(b);
        }
        (pbk::BYTES, Pv::B(b)) => o.base64(b),
        (pbk::INT, Pv::V(x)) => o.int(x as i64),
        (pbk::UINT, Pv::V(x)) => o.dec(x, false),
        (pbk::SINT, Pv::V(x)) => o.int(((x >> 1) as i64) ^ -((x & 1) as i64)),
        (pbk::BOOL, Pv::V(x)) => o.put(if x != 0 { b"true" } else { b"false" }),
        (pbk::TIMESTAMP, Pv::B(m)) => {
            let (s, n) = pb_timestamp(m, work).ok_or(())?;
            o.timestamp(s, n);
        }
        _ => return Err(()),
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum PbFrame<'m> {
    /// A message being written as an object: its descriptor entries left.
    Obj {
        m: &'m [u8],
        at: u16,
        left: u8,
        first: bool,
    },
    /// A repeated field being written as an array.
    Arr {
        m: &'m [u8],
        pos: usize,
        field: u32,
        kind: u8,
        arg: u8,
        first: bool,
    },
    /// A map field being written as an object.
    Map {
        m: &'m [u8],
        pos: usize,
        field: u32,
        arg: u8,
        first: bool,
    },
}

/// Why [`pb_json`] wrote nothing.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PbjFail {
    /// A bound was passed: the JSON did not fit the scratch, or nesting went
    /// past [`PBJ_MAX_FRAMES`]. The message may be well formed.
    Overflow,
    /// The message or descriptor does not decode, or `work` passed `budget`.
    Malformed,
}

/// Write `msg` as JSON by descriptor table `desc` into `buf`: the length
/// written, or why nothing was — a bound passed ([`PbjFail::Overflow`]) kept
/// apart from a message that does not decode ([`PbjFail::Malformed`]).
fn pb_json(
    desc: &[u8],
    msg: &[u8],
    buf: &mut [u8],
    work: &mut u64,
    budget: u64,
) -> Result<usize, PbjFail> {
    let d = PbDesc::parse(desc).ok_or(PbjFail::Malformed)?;
    let mut o = JsonOut {
        buf,
        n: 0,
        over: false,
    };
    let mut deep = false;
    match pb_json_write(&d, msg, &mut o, work, budget, &mut deep) {
        Some(n) if !o.over => Ok(n),
        // Output that ran past the scratch is an overflow whatever came
        // after it; so is nesting past the frame cap.
        _ if o.over || deep => Err(PbjFail::Overflow),
        _ => Err(PbjFail::Malformed),
    }
}

/// The body of [`pb_json`]: `None` on anything that does not decode or fit,
/// with `deep` set when nesting passed [`PBJ_MAX_FRAMES`] and `o.over` when
/// the output passed the scratch.
#[inline(never)]
fn pb_json_write(
    d: &PbDesc<'_>,
    msg: &[u8],
    o: &mut JsonOut<'_>,
    work: &mut u64,
    budget: u64,
    deep: &mut bool,
) -> Option<usize> {
    let mut frames = [PbFrame::Obj {
        m: &[],
        at: 0,
        left: 0,
        first: true,
    }; PBJ_MAX_FRAMES];
    let (left, at) = d.entries(0);
    frames[0] = PbFrame::Obj {
        m: msg,
        at,
        left,
        first: true,
    };
    let mut depth = 1usize;
    o.put1(b'{');
    // An open frame's separator, key and opening bracket for a new value.
    macro_rules! open {
        ($f:expr) => {{
            if depth >= PBJ_MAX_FRAMES {
                *deep = true;
                return None;
            }
            frames[depth] = $f;
            depth += 1;
        }};
    }
    while depth > 0 {
        if o.over || *work > budget {
            return None;
        }
        *work += 1;
        let top = depth - 1;
        match frames[top] {
            PbFrame::Obj { m, at, left, first } => {
                if left == 0 {
                    o.put1(b'}');
                    depth -= 1;
                    continue;
                }
                let (e, next) = d.entry(at);
                frames[top] = PbFrame::Obj {
                    m,
                    at: next,
                    left: left - 1,
                    first,
                };
                let k = e.kind & pbk::KIND;
                let sep = |o: &mut JsonOut<'_>| {
                    if !first {
                        o.put1(b',');
                    }
                    o.string(e.name);
                    o.put1(b':');
                };
                if k == pbk::MAP || e.kind & pbk::REPEATED != 0 {
                    if pb_find(m, 0, e.field, work).ok()?.is_none() {
                        continue;
                    }
                    sep(o);
                    frames[top] = PbFrame::Obj {
                        m,
                        at: next,
                        left: left - 1,
                        first: false,
                    };
                    if k == pbk::MAP {
                        o.put1(b'{');
                        open!(PbFrame::Map {
                            m,
                            pos: 0,
                            field: e.field,
                            arg: e.arg,
                            first: true
                        });
                    } else {
                        o.put1(b'[');
                        open!(PbFrame::Arr {
                            m,
                            pos: 0,
                            field: e.field,
                            kind: e.kind,
                            arg: e.arg,
                            first: true
                        });
                    }
                    continue;
                }
                let Some(v) = pb_last(m, e.field, work).ok()? else {
                    continue;
                };
                let Some((rk, ra, rv)) = pb_resolve(d, e.kind, e.arg, v, work).ok()? else {
                    continue;
                };
                if (e.kind & pbk::OMIT_DEFAULT != 0 || rk == pbk::TIMESTAMP)
                    && pb_default(rk, rv, work)
                {
                    continue;
                }
                sep(o);
                frames[top] = PbFrame::Obj {
                    m,
                    at: next,
                    left: left - 1,
                    first: false,
                };
                if rk == pbk::MSG {
                    let Pv::B(sub) = rv else { return None };
                    o.put1(b'{');
                    let (l, a) = d.entries(ra);
                    open!(PbFrame::Obj {
                        m: sub,
                        at: a,
                        left: l,
                        first: true
                    });
                } else {
                    pb_scalar(o, rk, rv, work).ok()?;
                }
            }
            PbFrame::Arr {
                m,
                pos,
                field,
                kind,
                arg,
                first,
            } => {
                let Some((v, next)) = pb_find(m, pos, field, work).ok()? else {
                    o.put1(b']');
                    depth -= 1;
                    continue;
                };
                frames[top] = PbFrame::Arr {
                    m,
                    pos: next,
                    field,
                    kind,
                    arg,
                    first: false,
                };
                let k = kind & pbk::KIND;
                let packed = matches!(
                    (k, v),
                    (pbk::INT | pbk::UINT | pbk::SINT | pbk::BOOL, Pv::B(_))
                );
                if packed {
                    let Pv::B(mut p) = v else { return None };
                    let mut f = first;
                    while !p.is_empty() {
                        if *work > budget {
                            return None;
                        }
                        let (x, n) = pb_varint(p, 0)?;
                        p = p.get(n..)?;
                        *work += 1;
                        if !f {
                            o.put1(b',');
                        }
                        f = false;
                        pb_scalar(o, k, Pv::V(x), work).ok()?;
                    }
                    frames[top] = PbFrame::Arr {
                        m,
                        pos: next,
                        field,
                        kind,
                        arg,
                        first: f,
                    };
                    continue;
                }
                if !first {
                    o.put1(b',');
                }
                match pb_resolve(d, kind, arg, v, work).ok()? {
                    None => o.put(b"null"),
                    Some((pbk::MSG, ra, Pv::B(sub))) => {
                        o.put1(b'{');
                        let (l, a) = d.entries(ra);
                        open!(PbFrame::Obj {
                            m: sub,
                            at: a,
                            left: l,
                            first: true
                        });
                    }
                    Some((rk, _, rv)) => pb_scalar(o, rk, rv, work).ok()?,
                }
            }
            PbFrame::Map {
                m,
                pos,
                field,
                arg,
                first,
            } => {
                let Some((v, next)) = pb_find(m, pos, field, work).ok()? else {
                    o.put1(b'}');
                    depth -= 1;
                    continue;
                };
                frames[top] = PbFrame::Map {
                    m,
                    pos: next,
                    field,
                    arg,
                    first: false,
                };
                let Pv::B(entry) = v else { return None };
                let ke = d.nth(arg, 0)?;
                let ve = d.nth(arg, 1)?;
                if !first {
                    o.put1(b',');
                }
                match (ke.kind & pbk::KIND, pb_last(entry, 1, work).ok()?) {
                    (pbk::STR, Some(Pv::B(key))) => {
                        pb_scalar(o, pbk::STR, Pv::B(key), work).ok()?
                    }
                    (pbk::STR, None) => o.put(b"\"\""),
                    (kk, key) => {
                        o.put1(b'"');
                        pb_scalar(o, kk, key.unwrap_or(Pv::V(0)), work).ok()?;
                        o.put1(b'"');
                    }
                }
                o.put1(b':');
                let vk = ve.kind & pbk::KIND;
                let val = pb_last(entry, 2, work).ok()?;
                match val.map(|x| pb_resolve(d, ve.kind, ve.arg, x, work)) {
                    Some(r) => match r.ok()? {
                        None => o.put(b"null"),
                        Some((pbk::MSG, ra, Pv::B(sub))) => {
                            o.put1(b'{');
                            let (l, a) = d.entries(ra);
                            open!(PbFrame::Obj {
                                m: sub,
                                at: a,
                                left: l,
                                first: true
                            });
                        }
                        Some((rk, _, rv)) => pb_scalar(o, rk, rv, work).ok()?,
                    },
                    // An absent value is its kind's default.
                    None => match vk {
                        pbk::STR | pbk::BYTES => o.put(b"\"\""),
                        pbk::BOOL => o.put(b"false"),
                        pbk::MSG => o.put(b"{}"),
                        pbk::INT | pbk::UINT | pbk::SINT => o.put1(b'0'),
                        _ => o.put(b"null"),
                    },
                }
            }
        }
    }
    if o.over {
        None
    } else {
        Some(o.n)
    }
}

fn hexval(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// The `idx`-th `delim`-separated part of `v`, delimiters between `open`
/// and `close` not counting (see [`rd::PARTB`]); EMPTY past the last.
fn part_bracketed(v: &[u8], delim: u8, open: u8, close: u8, idx: usize) -> &[u8] {
    let (mut depth, mut k, mut start) = (0usize, 0usize, 0usize);
    for (i, &c) in v.iter().enumerate() {
        match c {
            _ if c == open && open == close => depth ^= 1,
            _ if c == open => depth += 1,
            _ if c == close => depth = depth.saturating_sub(1),
            _ if c == delim && depth == 0 => {
                if k == idx {
                    return v.get(start..i).unwrap_or(&[]);
                }
                k += 1;
                start = i + 1;
            }
            _ => {}
        }
    }
    if k == idx {
        v.get(start..).unwrap_or(&[])
    } else {
        &[]
    }
}

/// A Chronicle record frame's field `want`, as [`rd::FIELD`] reads it — the
/// layout `pipeline_core.rs` `encode_frame` writes: `[n u8]` then `n` ×
/// `[number u8][type u8][len u16 LE][bytes]`, type 0 bytes, 1 i64, 3 a
/// nested message.
fn frame_field(f: &[u8], want: u8) -> Result<Value<'_>, EvalError> {
    let (&n, mut rest) = f.split_first().ok_or(EvalError::Truncated)?;
    for _ in 0..n {
        let [num, ty, l0, l1, tail @ ..] = rest else {
            return Err(EvalError::Truncated);
        };
        let len = u16::from_le_bytes([*l0, *l1]) as usize;
        if tail.len() < len {
            return Err(EvalError::Truncated);
        }
        let (v, next) = tail.split_at(len);
        rest = next;
        if *num == want {
            return match ty {
                0 => Ok(Value::Bytes(v)),
                3 => Ok(Value::Frame(v)),
                1 => {
                    let raw: [u8; 8] = v.try_into().map_err(|_| EvalError::TypeError)?;
                    Ok(Value::Int(i64::from_le_bytes(raw)))
                }
                _ => Err(EvalError::TypeError),
            };
        }
    }
    Ok(Value::Null)
}

/// Bytes of a value a reader can look inside.
fn rd_bytes<'a>(v: Value<'a>) -> Result<&'a [u8], EvalError> {
    match v {
        Value::Bytes(b) | Value::Frame(b) => Ok(b),
        Value::Str(s) => Ok(s.as_bytes()),
        Value::Null => Ok(&[]),
        _ => Err(EvalError::TypeError),
    }
}

fn rd_find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > hay.len() {
        return None;
    }
    (0..=hay.len() - needle.len()).find(|&i| &hay[i..i + needle.len()] == needle)
}

/// An HTTP header block's value for `name`, as [`rd::HDR`] reads it.
fn rd_header<'a>(block: &'a [u8], name: &[u8]) -> &'a [u8] {
    for line in block.split(|&c| c == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some(colon) = line.iter().position(|&c| c == b':') else {
            continue;
        };
        if line[..colon].eq_ignore_ascii_case(name) {
            let mut v = &line[colon + 1..];
            while let [b' ' | b'\t', rest @ ..] = v {
                v = rest;
            }
            while let [rest @ .., b' ' | b'\t'] = v {
                v = rest;
            }
            return v;
        }
    }
    &[]
}

/// Read one protobuf base-128 varint at `pos` in `buf`; returns `(value, next)`.
/// Bounded (≤10 bytes) and allocation-free; `None` on truncation or overrun.
fn pb_varint(buf: &[u8], mut pos: usize) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        let byte = *buf.get(pos)?;
        pos += 1;
        // The tenth byte carries bit 63 alone: more would not fit a u64.
        if shift == 63 && byte > 1 {
            return None;
        }
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((result, pos));
        }
        shift += 7;
        if shift >= 64 {
            return None; // malformed: varint longer than 64 bits
        }
    }
}

const DEC_STACK: usize = 32;

/// Decimal digits of `v` into `out`; the length.
fn rd_dec_digits(v: i64, out: &mut [u8; 20]) -> usize {
    let mut tmp = [0u8; 20];
    let mut i = tmp.len();
    let mut u = v.unsigned_abs();
    loop {
        i -= 1;
        tmp[i] = b'0' + (u % 10) as u8;
        u /= 10;
        if u == 0 || i == 1 {
            break;
        }
    }
    if v < 0 {
        i -= 1;
        tmp[i] = b'-';
    }
    let n = tmp.len() - i;
    out[..n].copy_from_slice(&tmp[i..]);
    n
}

/// Execute a deserialization program over `input`, constructing a record into
/// `builder`. Values pushed by read opcodes borrow `input` (`'a`), so the
/// constructed message does too — no allocation.
pub fn eval_decode<'a>(
    code: &'a [u8],
    input: &'a [u8],
    builder: &mut Builder<'a>,
    max_cost: u64,
) -> Result<(), EvalError> {
    eval_decode_scratch(code, input, &mut [], builder, max_cost)
}

/// [`eval_decode`] with a SCRATCH arena for the ops that build a value rather
/// than borrow one (`CAT`, `HEX`, `JSONDEF`, `PCTDEC`, `PBJSON`). Each built
/// value takes the front of what is left, so it lives as long as the input the
/// others borrow — the record is built without an allocation either way. A
/// program using none of them needs none.
pub fn eval_decode_scratch<'a>(
    code: &'a [u8],
    input: &'a [u8],
    scratch: &'a mut [u8],
    builder: &mut Builder<'a>,
    max_cost: u64,
) -> Result<(), EvalError> {
    let mut free: &'a mut [u8] = scratch;
    let mut locals: [Value<'a>; MAX_LOCALS] = [Value::Null; MAX_LOCALS];
    let mut stack: [Value<'a>; DEC_STACK] = [Value::Null; DEC_STACK];
    let mut sp: usize = 0;
    let mut pos: usize = 0; // cursor into `input`
    let mut pc: usize = 0;
    let mut cost: u64 = 0;

    macro_rules! push {
        ($v:expr) => {{
            if sp >= DEC_STACK {
                return Err(EvalError::StackOverflow);
            }
            stack[sp] = $v;
            sp += 1;
        }};
    }
    macro_rules! pop {
        () => {{
            if sp == 0 {
                return Err(EvalError::StackUnderflow);
            }
            sp -= 1;
            stack[sp]
        }};
    }
    macro_rules! take {
        ($n:expr) => {{
            let n = $n;
            let slice = pos
                .checked_add(n)
                .and_then(|end| input.get(pos..end))
                .ok_or(EvalError::Truncated)?;
            pos += n;
            slice
        }};
    }

    loop {
        if pc >= code.len() {
            return Err(EvalError::Truncated);
        }
        cost += 1;
        if cost > max_cost {
            return Err(EvalError::CostExceeded);
        }
        let opcode = code[pc];
        pc += 1;

        // Shared immediate/arithmetic ops (see vm_core.rs). The deserializer has no
        // params, so it does not use `load_op` (LOAD_PARAM/GET_FIELD stay unknown).
        if let Some(res) = arith_op(opcode, code, &mut pc, &mut stack, &mut sp) {
            res?;
            continue;
        }

        match opcode {
            op::FINISH_MSG => return Ok(()),
            // A literal — a key separator, a scheme prefix — to build with.
            op::PUSH_STR => {
                let b = code.get(pc..pc + 2).ok_or(EvalError::Truncated)?;
                let len = u16::from_le_bytes([b[0], b[1]]) as usize;
                pc += 2;
                let bytes = code.get(pc..pc + len).ok_or(EvalError::Truncated)?;
                pc += len;
                push!(Value::Bytes(bytes));
            }
            op::STORE_LOCAL => {
                let idx = *code.get(pc).ok_or(EvalError::Truncated)? as usize;
                pc += 1;
                if idx >= MAX_LOCALS {
                    return Err(EvalError::TypeError);
                }
                locals[idx] = pop!();
            }
            op::LOAD_LOCAL => {
                let idx = *code.get(pc).ok_or(EvalError::Truncated)? as usize;
                pc += 1;
                if idx >= MAX_LOCALS {
                    return Err(EvalError::TypeError);
                }
                push!(locals[idx]);
            }
            rd::BEFORE | rd::AFTER | rd::HDR => {
                let b = code.get(pc..pc + 2).ok_or(EvalError::Truncated)?;
                let len = u16::from_le_bytes([b[0], b[1]]) as usize;
                pc += 2;
                let arg = code.get(pc..pc + len).ok_or(EvalError::Truncated)?;
                pc += len;
                let v = rd_bytes(pop!())?;
                let out: &'a [u8] = match opcode {
                    rd::BEFORE => match rd_find(v, arg) {
                        Some(i) => &v[..i],
                        None => v,
                    },
                    rd::AFTER => match rd_find(v, arg) {
                        Some(i) => &v[i + arg.len()..],
                        None => &[],
                    },
                    _ => rd_header(v, arg), // HDR
                };
                push!(Value::Bytes(out));
            }
            rd::FIELD => {
                let want = *code.get(pc).ok_or(EvalError::Truncated)?;
                pc += 1;
                let f = rd_bytes(pop!())?;
                push!(frame_field(f, want)?);
            }
            rd::JSONAT => {
                let path = rd_bytes(pop!())?;
                let doc = rd_bytes(pop!())?;
                push!(match json_locate(doc, path) {
                    Some(span) => {
                        let (i, e) = json_unquote(doc, span);
                        Value::Bytes(doc.get(i..e).unwrap_or(&[]))
                    }
                    None => Value::Null,
                });
            }
            rd::JSONDEF => {
                let raw = rd_bytes(pop!())?;
                let path = rd_bytes(pop!())?;
                let docv = pop!();
                let doc = rd_bytes(docv)?;
                match json_default_at(doc, path) {
                    Some((obj_at, key, empty)) if !raw.is_empty() => {
                        // doc[..=obj_at] "key":raw [,] doc[obj_at+1..]
                        let k = path.get(key).unwrap_or(&[]);
                        let (head_doc, tail_doc) = doc.split_at((obj_at + 1).min(doc.len()));
                        let comma: &[u8] = if empty { b"" } else { b"," };
                        let need = doc.len() + 1 + k.len() + 2 + raw.len() + comma.len();
                        if need > free.len() {
                            return Err(EvalError::ScratchOverflow);
                        }
                        let (head, tail) = core::mem::take(&mut free).split_at_mut(need);
                        free = tail;
                        let bytes = head_doc
                            .iter()
                            .chain(b"\"")
                            .chain(k)
                            .chain(b"\":")
                            .chain(raw)
                            .chain(comma)
                            .chain(tail_doc);
                        for (d, &c) in head.iter_mut().zip(bytes) {
                            *d = c;
                        }
                        let built: &'a [u8] = head;
                        push!(Value::Bytes(built));
                    }
                    _ => push!(docv),
                }
            }
            rd::PARTB => {
                let b = code.get(pc..pc + 4).ok_or(EvalError::Truncated)?;
                let (delim, open, close, idx) = (b[0], b[1], b[2], b[3] as usize);
                pc += 4;
                let v = rd_bytes(pop!())?;
                push!(Value::Bytes(part_bracketed(v, delim, open, close, idx)));
            }
            rd::PBJSON => {
                let desc = rd_bytes(pop!())?;
                let msg = rd_bytes(pop!())?;
                let buf = core::mem::take(&mut free);
                let mut work = 0u64;
                let budget = max_cost.saturating_sub(cost);
                let written = pb_json(desc, msg, buf, &mut work, budget);
                cost = cost.saturating_add(work);
                if cost > max_cost {
                    return Err(EvalError::CostExceeded);
                }
                match written {
                    Ok(n) if n <= buf.len() => {
                        let (head, tail) = buf.split_at_mut_checked(n).unwrap_or_default();
                        free = tail;
                        let built: &'a [u8] = head;
                        push!(Value::Bytes(built));
                    }
                    Err(PbjFail::Overflow) => return Err(EvalError::ScratchOverflow),
                    _ => {
                        free = buf;
                        push!(Value::Null);
                    }
                }
            }
            rd::PCTDEC => {
                let v = rd_bytes(pop!())?;
                // Decoding only shortens, so the input's length bounds it.
                if v.len() > free.len() {
                    return Err(EvalError::ScratchOverflow);
                }
                let (head, tail) = core::mem::take(&mut free).split_at_mut(v.len());
                free = tail;
                let (mut r, mut w) = (0usize, 0usize);
                while let Some(&c) = v.get(r) {
                    let (b, step) = match c {
                        b'+' => (b' ', 1),
                        b'%' => match (
                            v.get(r + 1).and_then(|&h| hexval(h)),
                            v.get(r + 2).and_then(|&l| hexval(l)),
                        ) {
                            (Some(h), Some(l)) => ((h << 4) | l, 3),
                            _ => (b'%', 1),
                        },
                        _ => (c, 1),
                    };
                    if let Some(d) = head.get_mut(w) {
                        *d = b;
                    }
                    w += 1;
                    r += step;
                }
                let built: &'a [u8] = head;
                push!(Value::Bytes(built.get(..w).unwrap_or(&[])));
            }
            rd::PART => {
                let delim = *code.get(pc).ok_or(EvalError::Truncated)?;
                let idx = *code.get(pc + 1).ok_or(EvalError::Truncated)? as usize;
                pc += 2;
                let v = rd_bytes(pop!())?;
                let part = v.split(|&c| c == delim).nth(idx).unwrap_or(&[]);
                push!(Value::Bytes(part));
            }
            rd::COALESCE => {
                let b = pop!();
                let a = pop!();
                let pick = if rd_bytes(a)?.is_empty() { b } else { a };
                push!(pick);
            }
            rd::GATE => {
                let cond = rd_bytes(pop!())?;
                let v = pop!();
                push!(if cond.is_empty() {
                    Value::Bytes(&[])
                } else {
                    v
                });
            }
            rd::DROP => {
                let b = code.get(pc..pc + 2).ok_or(EvalError::Truncated)?;
                let n = u16::from_le_bytes([b[0], b[1]]) as usize;
                pc += 2;
                let v = rd_bytes(pop!())?;
                push!(Value::Bytes(v.get(n..).unwrap_or(&[])));
            }
            rd::CAT | rd::HEX => {
                let n = if opcode == rd::CAT {
                    let n = *code.get(pc).ok_or(EvalError::Truncated)? as usize;
                    pc += 1;
                    n
                } else {
                    1
                };
                if n == 0 || n > sp {
                    return Err(EvalError::StackUnderflow);
                }
                // Measure, then build into the front of what scratch is left.
                // Every operand is first its text — an integer's decimal
                // digits, anything else its bytes — and `HEX` then doubles it.
                let per = if opcode == rd::HEX { 2 } else { 1 };
                let mut need = 0usize;
                for &v in &stack[sp - n..sp] {
                    need += per
                        * match v {
                            Value::Int(v) => {
                                let mut d = [0u8; 20];
                                rd_dec_digits(v, &mut d)
                            }
                            other => rd_bytes(other)?.len(),
                        };
                }
                if need > free.len() {
                    return Err(EvalError::ScratchOverflow);
                }
                let (head, tail) = core::mem::take(&mut free).split_at_mut(need);
                free = tail;
                let mut w = 0usize;
                for &v in &stack[sp - n..sp] {
                    let mut d = [0u8; 20];
                    let text: &[u8] = match v {
                        Value::Int(v) => {
                            let dl = rd_dec_digits(v, &mut d);
                            &d[..dl]
                        }
                        other => rd_bytes(other)?,
                    };
                    if opcode == rd::HEX {
                        const HX: &[u8; 16] = b"0123456789abcdef";
                        for &c in text {
                            head[w] = HX[(c >> 4) as usize];
                            head[w + 1] = HX[(c & 0x0f) as usize];
                            w += 2;
                        }
                    } else {
                        head[w..w + text.len()].copy_from_slice(text);
                        w += text.len();
                    }
                }
                sp -= n;
                let built: &'a [u8] = head;
                push!(Value::Bytes(built));
            }
            op::SET_FIELD => {
                let number = read_u32(code, pc)?;
                pc += 4;
                let value = pop!();
                if builder.len >= MAX_BUILD_FIELDS {
                    return Err(EvalError::BuildOverflow);
                }
                builder.fields[builder.len] = Field { number, value };
                builder.len += 1;
            }
            rd::SKIP => {
                let b = code.get(pc..pc + 2).ok_or(EvalError::Truncated)?;
                let n = u16::from_le_bytes([b[0], b[1]]) as usize;
                pc += 2;
                let _ = take!(n);
            }
            rd::LIT => {
                let b = code.get(pc..pc + 2).ok_or(EvalError::Truncated)?;
                let len = u16::from_le_bytes([b[0], b[1]]) as usize;
                pc += 2;
                let expect = code.get(pc..pc + len).ok_or(EvalError::Truncated)?;
                pc += len;
                let got = take!(len);
                if got != expect {
                    return Err(EvalError::TypeError); // reply did not match the expected literal
                }
            }
            rd::UNTIL => {
                let delim = *code.get(pc).ok_or(EvalError::Truncated)?;
                pc += 1;
                let start = pos;
                while pos < input.len() && input[pos] != delim {
                    pos += 1;
                }
                if pos >= input.len() {
                    return Err(EvalError::Truncated);
                }
                let slice = &input[start..pos];
                pos += 1; // skip the delimiter
                push!(Value::Bytes(slice));
            }
            rd::UNTIL_OPT => {
                let delim = *code.get(pc).ok_or(EvalError::Truncated)?;
                pc += 1;
                let start = pos;
                while pos < input.len() && input[pos] != delim {
                    pos += 1;
                }
                let slice = &input[start..pos];
                if pos < input.len() {
                    pos += 1; // consume the delimiter
                }
                // No `Truncated`: running out IS one of the outcomes. An empty
                // push at the end of input is what pads a short sequence.
                push!(Value::Bytes(slice));
            }
            rd::TAKE => {
                let b = code.get(pc..pc + 2).ok_or(EvalError::Truncated)?;
                let n = u16::from_le_bytes([b[0], b[1]]) as usize;
                pc += 2;
                push!(Value::Bytes(take!(n)));
            }
            rd::TAKEN => {
                // A negative count is not a count.
                let n = match pop!() {
                    Value::Int(i) => u64::try_from(i).map_err(|_| EvalError::TypeError)?,
                    Value::Uint(u) => u,
                    _ => return Err(EvalError::TypeError),
                };
                // A count past the address space is more than the input holds.
                let n = usize::try_from(n).map_err(|_| EvalError::Truncated)?;
                push!(Value::Bytes(take!(n)));
            }
            rd::INT => {
                let width = *code.get(pc).ok_or(EvalError::Truncated)? as usize;
                let endian = *code.get(pc + 1).ok_or(EvalError::Truncated)?;
                pc += 2;
                if width == 0 || width > 8 {
                    return Err(EvalError::TypeError);
                }
                let bytes = take!(width);
                let mut v: i64 = 0;
                if endian == 1 {
                    // little-endian
                    let mut k = width;
                    while k > 0 {
                        k -= 1;
                        v = (v << 8) | bytes[k] as i64;
                    }
                } else {
                    let mut k = 0;
                    while k < width {
                        v = (v << 8) | bytes[k] as i64;
                        k += 1;
                    }
                }
                push!(Value::Int(v));
            }
            rd::SEEK => {
                let b = code.get(pc..pc + 2).ok_or(EvalError::Truncated)?;
                let len = u16::from_le_bytes([b[0], b[1]]) as usize;
                pc += 2;
                let seq = code.get(pc..pc + len).ok_or(EvalError::Truncated)?;
                pc += len;
                if len == 0 {
                    return Err(EvalError::TypeError);
                }
                // Advance the cursor to just past the first occurrence of `seq`.
                let mut found = false;
                while pos + len <= input.len() {
                    if &input[pos..pos + len] == seq {
                        pos += len;
                        found = true;
                        break;
                    }
                    pos += 1;
                }
                if !found {
                    return Err(EvalError::Truncated); // sequence not present
                }
            }
            rd::REST => {
                let slice = input.get(pos..).ok_or(EvalError::Truncated)?;
                pos = input.len();
                push!(Value::Bytes(slice));
            }
            rd::H2MSG => {
                // HTTP/2 frame: [len:3 BE][type:1][flags:1][r+stream:4][payload].
                // Walk frames from the cursor to the first DATA frame (type 0), then
                // read its gRPC Length-Prefixed-Message: [compressed:1][len:4 BE][msg].
                // The loop exits only by finding DATA (push + break) or running off
                // the end of `input` (a `?` error) — a missing DATA frame is Truncated.
                // A PADDED DATA frame, or a message whose compressed flag is set,
                // is refused (`TypeError`): neither is read as plain protobuf.
                loop {
                    let hdr = input.get(pos..pos + 9).ok_or(EvalError::Truncated)?;
                    let flen =
                        ((hdr[0] as usize) << 16) | ((hdr[1] as usize) << 8) | hdr[2] as usize;
                    let ftype = hdr[3];
                    let body_start = pos + 9;
                    let body = input
                        .get(body_start..body_start + flen)
                        .ok_or(EvalError::Truncated)?;
                    pos = body_start + flen;
                    if ftype == 0x00 {
                        // DATA frame: parse the single length-prefixed gRPC message.
                        if hdr[4] & 0x08 != 0 {
                            return Err(EvalError::TypeError); // PADDED
                        }
                        if body.len() < 5 {
                            return Err(EvalError::Truncated);
                        }
                        if body[0] != 0 {
                            return Err(EvalError::TypeError); // compressed
                        }
                        let mlen = u32::from_be_bytes([body[1], body[2], body[3], body[4]]);
                        let msg = usize::try_from(mlen)
                            .ok()
                            .and_then(|l| l.checked_add(5))
                            .and_then(|end| body.get(5..end))
                            .ok_or(EvalError::Truncated)?;
                        push!(Value::Bytes(msg));
                        break;
                    }
                }
            }
            rd::PBFIELD => {
                let target = read_u32(code, pc)?;
                pc += 4;
                let msg = match pop!() {
                    Value::Bytes(b) => b,
                    Value::Str(s) => s.as_bytes(),
                    _ => return Err(EvalError::TypeError),
                };
                // The first occurrence of `target`; bounded by the message.
                let mut work = 0u64;
                let found = pb_find(msg, 0, target, &mut work).map_err(|_| EvalError::Truncated)?;
                push!(match found {
                    Some((Pv::V(v) | Pv::F(v), _)) => Value::Int(v as i64),
                    Some((Pv::B(b), _)) => Value::Bytes(b),
                    None => Value::Null,
                });
            }
            rd::DECINT => {
                let neg = input.get(pos) == Some(&b'-');
                if neg {
                    pos += 1;
                }
                // Accumulated toward the sign, so i64::MIN reads; a number
                // outside i64 is refused, never wrapped.
                let mut v: i64 = 0;
                let mut any = false;
                while let Some(&c) = input.get(pos) {
                    if !c.is_ascii_digit() {
                        break;
                    }
                    let d = (c - b'0') as i64;
                    v = v
                        .checked_mul(10)
                        .and_then(|x| {
                            if neg {
                                x.checked_sub(d)
                            } else {
                                x.checked_add(d)
                            }
                        })
                        .ok_or(EvalError::Overflow)?;
                    pos += 1;
                    any = true;
                }
                if !any {
                    return Err(EvalError::TypeError);
                }
                push!(Value::Int(v));
            }
            other => return Err(EvalError::BadOpcode(other)),
        }
    }
}
