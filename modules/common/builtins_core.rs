// CEL extension builtins — the `CALL` opcode's dispatch table and
// implementations. Pure, dependency-free, no_std; `include!`d wherever
// `vm_core.rs` is (all five engines + the host crate), same one-source rule.
//
// THE SURFACE IS PINNED, NOT INVENTED. Functions are the ASCII/byte-scoped
// subset of CEL's standard library and its versioned extension libraries
// (`cel-go/ext`): `strings` (ext version 3), `math` (integer subset),
// `encoders`. `docs/bytecode_policy.md` carries the pinned table including
// every deviation; the load-bearing ones, stated once here:
//
//   * indices and sizes are BYTE offsets, not code points — identical to CEL
//     for ASCII, documented deviation beyond it;
//   * `trim` trims ASCII whitespace, consistent with `lowerAscii`/
//     `upperAscii` (which CEL itself scoped to ASCII to avoid locale tables);
//   * `reverse` is overloaded by STATIC type: on `str` it reverses UTF-8
//     code points (CEL-conformant — "héllo" → "olléh"), on `bytes` it
//     reverses bytes. The compiler resolves the overload; the runtime keys
//     off the builtin id, never sniffs content.
//
// GOVERNANCE: the table is append-only. An id, once published, keeps its
// meaning forever — ids are wire contract exactly like content-type bytes.
// Each entry is gated by its extension's cfg feature, one per variant;
// a `CALL` naming an id that is absent from this build fails closed with
// `EvalError::BadBuiltin` — never a silent identity, never a guess.
//
// OUTPUT DISCIPLINE: functions that produce bytes which do not exist in the
// input (`reverse`, case mapping, `replace`, base64) append into the caller's
// bounded scratch arena and return `Value::Scratch{off,len}`. Functions that
// can answer with a SUBSLICE of an argument (`substring`, `trim`, `charAt`)
// do so — zero copies. Predicates and indexes return scalars. Overflowing
// the arena is `EvalError::ScratchOverflow`: fail closed, like every bound.
//
// BORROW DISCIPLINE (why `Source` exists): a result slice borrowed out of
// the arena would freeze the arena against the very append that produces the
// next result. So arena-backed values travel as offsets (`Value::Scratch`),
// and every implementation reads its input through `Source` — an external
// slice (borrowing the input, disjoint from the arena) or an offset window
// into the arena's frozen prefix. Reads are index-wise; no borrow is held
// across a write; the core stays entirely safe Rust.

/// Builtin ids. Append-only; never reorder, never reuse.
pub mod builtin {
    // ── strings (feature "strings") ─────────────────────────────────────
    pub const SIZE: u16 = 1; //  (str|bytes) -> int          [byte length]
    pub const CONTAINS: u16 = 2; //  (s, sub) -> bool
    pub const STARTS_WITH: u16 = 3; //  (s, prefix) -> bool
    pub const ENDS_WITH: u16 = 4; //  (s, suffix) -> bool
    pub const INDEX_OF: u16 = 5; //  (s, sub) -> int         [-1 = absent]
    pub const LAST_INDEX_OF: u16 = 6; //  (s, sub) -> int
    pub const CHAR_AT: u16 = 7; //  (s, i) -> str            [1-byte slice]
    pub const SUBSTRING: u16 = 8; //  (s, start) -> str      [suffix]
    pub const SUBSTRING_RANGE: u16 = 9; //  (s, start, end) -> str
    pub const TRIM: u16 = 10; //  (s) -> str                 [ASCII ws]
    pub const REVERSE_STR: u16 = 11; //  (str) -> str        [code points]
    pub const REVERSE_BYTES: u16 = 12; //  (bytes) -> bytes  [bytes]
    pub const LOWER_ASCII: u16 = 13; //  (s) -> str
    pub const UPPER_ASCII: u16 = 14; //  (s) -> str
    pub const REPLACE: u16 = 15; //  (s, from, to) -> str    [all; from≠""]
                                 // ── math (feature "math"; int/uint only) ────────────────────────────
    pub const MATH_GREATEST: u16 = 16; //  (a, b) -> int     [2-arg pin]
    pub const MATH_LEAST: u16 = 17; //  (a, b) -> int
    pub const MATH_ABS: u16 = 18; //  (a) -> int             [i64::MIN errs]
    pub const MATH_SIGN: u16 = 19; //  (a) -> int
    pub const BIT_AND: u16 = 20; //  (a, b) -> int
    pub const BIT_OR: u16 = 21; //  (a, b) -> int
    pub const BIT_XOR: u16 = 22; //  (a, b) -> int
    pub const BIT_SHL: u16 = 23; //  (a, n) -> int           [n∉0..64 errs]
    pub const BIT_SHR: u16 = 24; //  (a, n) -> int
                                 // ── encoders (feature "encoders") ───────────────────────────────────
    pub const B64_ENCODE: u16 = 25; //  (str|bytes) -> str   [std alphabet, pad]
    pub const B64_DECODE: u16 = 26; //  (str) -> bytes       [strict]
                                    // ── documents (feature "strings") ───────────────────────────────────
    pub const JSON_GET: u16 = 27; //  json.get(doc, path) -> str   [EMPTY when absent]
    pub const JSON_HAS: u16 = 28; //  json.has(doc, path) -> bool
    pub const JSON_SET_DEFAULT: u16 = 29; //  json.setDefault(doc, path, raw) -> str
    pub const PART: u16 = 30; //  (s, delim, i) -> str      [the i-th part; EMPTY past the end]
    pub const CONCAT: u16 = 31; //  a + b (strings) -> str      [scratch]
}

/// Bounded output arena for scratch-producing builtins. The caller owns the
/// storage; results reference it by offset (`Value::Scratch`). `used` only
/// grows during one evaluation — earlier results stay valid at their offsets
/// (the "frozen prefix").
pub struct Scratch<'s> {
    pub buf: &'s mut [u8],
    pub used: usize,
}

impl<'s> Scratch<'s> {
    pub fn new(buf: &'s mut [u8]) -> Self {
        Self { buf, used: 0 }
    }
    /// Reserve `n` bytes; `Ok(offset)` or fail closed.
    fn reserve(&mut self, n: usize) -> Result<usize, EvalError> {
        let off = self.used;
        if n > self.buf.len() - off {
            return Err(EvalError::ScratchOverflow);
        }
        self.used = off + n;
        Ok(off)
    }
    /// The bytes of a finished scratch value (for callers reading results).
    pub fn slice(&self, off: u32, len: u32) -> &[u8] {
        &self.buf[off as usize..(off + len) as usize]
    }
}

/// Whether THIS BUILD carries `id` — the load-time mirror of `call_builtin`'s
/// dispatch: an id that is pinned but compiled out (extension feature off)
/// is unavailable. Engines check every `CALL` in a program at LOAD, so a
/// program authored for `full` fails a subset engine loudly at init — named
/// once — instead of per-record forever.
pub fn builtin_available(id: u16) -> bool {
    use builtin::*;
    match id {
        #[cfg(feature = "strings")]
        SIZE | CONTAINS | STARTS_WITH | ENDS_WITH | INDEX_OF | LAST_INDEX_OF | CHAR_AT
        | SUBSTRING | SUBSTRING_RANGE | TRIM | REVERSE_STR | REVERSE_BYTES | LOWER_ASCII
        | UPPER_ASCII | REPLACE | JSON_GET | JSON_HAS | JSON_SET_DEFAULT | PART | CONCAT => true,
        #[cfg(feature = "math")]
        MATH_GREATEST | MATH_LEAST | MATH_ABS | MATH_SIGN | BIT_AND | BIT_OR | BIT_XOR
        | BIT_SHL | BIT_SHR => true,
        #[cfg(feature = "encoders")]
        B64_ENCODE | B64_DECODE => true,
        _ => false,
    }
}

/// Whether this build carries the `cel.bind` local-slot opcodes.
pub const BINDINGS_AVAILABLE: bool = cfg!(feature = "bindings");

/// The argument count a builtin pops (compiler and runtime must agree; the
/// runtime re-derives it here so a corrupt program cannot desynchronise them).
pub fn builtin_arity(id: u16) -> Option<usize> {
    use builtin::*;
    Some(match id {
        SIZE | TRIM | REVERSE_STR | REVERSE_BYTES | LOWER_ASCII | UPPER_ASCII | MATH_ABS
        | MATH_SIGN | B64_ENCODE | B64_DECODE => 1,
        CONTAINS | STARTS_WITH | ENDS_WITH | INDEX_OF | LAST_INDEX_OF | CHAR_AT | SUBSTRING
        | MATH_GREATEST | MATH_LEAST | BIT_AND | BIT_OR | BIT_XOR | BIT_SHL | BIT_SHR
        | JSON_GET | JSON_HAS | CONCAT => 2,
        SUBSTRING_RANGE | REPLACE | JSON_SET_DEFAULT | PART => 3,
        _ => return None,
    })
}

/// Where a string-ish argument's bytes live. `Ext` borrows the evaluation
/// input (disjoint from the arena, so the arena stays mutable); `Arena` is an
/// offset window into the frozen prefix.
#[derive(Clone, Copy)]
enum Source<'a> {
    Ext(&'a [u8]),
    Arena { off: usize, len: usize },
}

impl<'a> Source<'a> {
    fn of(v: &Value<'a>) -> Result<Self, EvalError> {
        match v {
            Value::Str(x) => Ok(Source::Ext(x.as_bytes())),
            Value::Bytes(x) => Ok(Source::Ext(x)),
            Value::Scratch { off, len } => Ok(Source::Arena {
                off: *off as usize,
                len: *len as usize,
            }),
            _ => Err(EvalError::TypeError),
        }
    }
    fn len(&self) -> usize {
        match self {
            Source::Ext(x) => x.len(),
            Source::Arena { len, .. } => *len,
        }
    }
    /// The source's bytes, borrowed from wherever they live. For an arena value
    /// the borrow is of `s`, so it has to end before `s` is written — callers
    /// take positions from it first and write afterwards.
    fn bytes<'b>(&'b self, s: &'b Scratch<'_>) -> &'b [u8]
    where
        'a: 'b,
    {
        match self {
            Source::Ext(x) => x,
            Source::Arena { off, len } => &s.buf[*off..*off + *len],
        }
    }
    #[inline]
    fn at(&self, i: usize, s: &Scratch<'_>) -> u8 {
        match self {
            Source::Ext(x) => x[i],
            Source::Arena { off, .. } => s.buf[off + i],
        }
    }
    /// Byte-wise window equality against `other` at `self[i..i+n]`.
    fn window_eq(&self, i: usize, other: &Source<'a>, s: &Scratch<'_>) -> bool {
        let n = other.len();
        if i + n > self.len() {
            return false;
        }
        let mut k = 0;
        while k < n {
            if self.at(i + k, s) != other.at(k, s) {
                return false;
            }
            k += 1;
        }
        true
    }
    /// Re-window this source to `[lo, hi)` as a VALUE — a subslice for `Ext`,
    /// an offset re-window for `Arena` (both zero-copy).
    fn window_value(&self, lo: usize, hi: usize) -> Value<'a> {
        match self {
            Source::Ext(x) => Value::Bytes(&x[lo..hi]),
            Source::Arena { off, .. } => Value::Scratch {
                off: (*off + lo) as u32,
                len: (hi - lo) as u32,
            },
        }
    }
}

fn arg_int(v: &Value<'_>) -> Result<i64, EvalError> {
    match v {
        Value::Int(i) => Ok(*i),
        Value::Uint(u) => Ok(*u as i64),
        _ => Err(EvalError::TypeError),
    }
}

/// First byte index of `needle` in `hay`, or -1. Empty needle → 0 (CEL).
fn find(hay: &Source<'_>, needle: &Source<'_>, s: &Scratch<'_>) -> i64 {
    if needle.len() == 0 {
        return 0;
    }
    if needle.len() > hay.len() {
        return -1;
    }
    let mut i = 0;
    while i + needle.len() <= hay.len() {
        if hay.window_eq(i, needle, s) {
            return i as i64;
        }
        i += 1;
    }
    -1
}

/// Last byte index of `needle` in `hay`, or -1. Empty needle → len (CEL).
fn rfind(hay: &Source<'_>, needle: &Source<'_>, s: &Scratch<'_>) -> i64 {
    if needle.len() == 0 {
        return hay.len() as i64;
    }
    if needle.len() > hay.len() {
        return -1;
    }
    let mut i = hay.len() - needle.len();
    loop {
        if hay.window_eq(i, needle, s) {
            return i as i64;
        }
        if i == 0 {
            return -1;
        }
        i -= 1;
    }
}

fn is_ascii_ws(b: u8) -> bool {
    b == b' ' || b == b'\t' || b == b'\n' || b == b'\r' || b == 0x0b || b == 0x0c
}

/// A non-negative index within `0..=len`, else TypeError (CEL errors on
/// out-of-range rather than clamping).
///
/// Range-checked as an `i64` before it becomes a `usize`. Converting first would
/// truncate on a 32-bit core, so an index of 2^32 would read as 0 on an RP part
/// and be out of range on a Pi 5 — one program, two answers.
fn checked_index(i: i64, len: usize) -> Result<usize, EvalError> {
    match usize::try_from(i) {
        Ok(u) if u <= len => Ok(u),
        _ => Err(EvalError::TypeError),
    }
}

/// UTF-8 sequence length claimed by a lead byte (1 for ASCII and invalid).
fn utf8_claim(lead: u8) -> usize {
    if lead & 0b1110_0000 == 0b1100_0000 {
        2
    } else if lead & 0b1111_0000 == 0b1110_0000 {
        3
    } else if lead & 0b1111_1000 == 0b1111_0000 {
        4
    } else {
        1
    }
}

const B64_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn b64_val(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

// ---- JSON: a bounded path reader over a document's bytes -------------------
//
// Shared by the `json.*` builtins and the decoder's `rd::JSON`. It reads where
// the document lives and allocates nothing. Every step moves forward through the
// bytes — a member or element that is not followed by `,` or its closer ends the
// read as absent — so the work is bounded by the document's length, whatever
// index or path a caller names. Keys are compared as written: a key the document
// spells with an escape is matched only by a path spelling it the same way.

/// Skip JSON whitespace from `i`.
fn json_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\n' | b'\r') {
        i += 1;
    }
    i
}

/// The end of the JSON value starting at `i` (bounded by the bytes: a
/// malformed document ends early rather than overruns).
fn json_skip(b: &[u8], i: usize) -> usize {
    let i = json_ws(b, i);
    if i >= b.len() {
        return i;
    }
    match b[i] {
        b'"' => {
            let mut j = i + 1;
            while j < b.len() && b[j] != b'"' {
                if b[j] == b'\\' {
                    j += 1;
                }
                j += 1;
            }
            (j + 1).min(b.len())
        }
        b'{' | b'[' => {
            let mut depth = 0usize;
            let mut j = i;
            while j < b.len() {
                match b[j] {
                    b'"' => {
                        j = json_skip(b, j);
                        continue;
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth == 0 {
                            return j + 1;
                        }
                    }
                    _ => {}
                }
                j += 1;
            }
            j
        }
        _ => {
            let mut j = i;
            while j < b.len() && !matches!(b[j], b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r')
            {
                j += 1;
            }
            j
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
                let ke = json_skip(b, j);
                let key = b.get(j + 1..ke.checked_sub(1)?)?;
                j = json_ws(b, ke);
                if b.get(j) != Some(&b':') {
                    return None;
                }
                let vs = json_ws(b, j + 1);
                if key == seg {
                    return Some(vs);
                }
                j = json_ws(b, json_skip(b, vs));
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
                let e = json_skip(b, j);
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
/// decimal array indices), or `None` when absent. Empty segments are skipped, so
/// an empty path names the document itself.
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
    Some((i, json_skip(doc, i)))
}

/// A span as a value reads: a string's content without its quotes, anything
/// else as written.
fn json_unquote(doc: &[u8], (i, e): (usize, usize)) -> (usize, usize) {
    if e >= i + 2 && doc[i] == b'"' {
        (i + 1, e - 1)
    } else {
        (i, e)
    }
}

/// The value at a dotted path as [`json_unquote`] reads it; EMPTY when absent.
fn json_value<'a>(doc: &'a [u8], path: &[u8]) -> &'a [u8] {
    match json_locate(doc, path) {
        Some(span) => {
            let (i, e) = json_unquote(doc, span);
            &doc[i..e]
        }
        None => &[],
    }
}

/// Append `src[lo..hi]` into the scratch at `*w` (a reserved range).
fn put_src(src: &Source<'_>, lo: usize, hi: usize, s: &mut Scratch<'_>, w: &mut usize) {
    let mut i = lo;
    while i < hi {
        let b = src.at(i, s);
        s.buf[*w] = b;
        *w += 1;
        i += 1;
    }
}

/// Append literal bytes into the scratch at `*w` (a reserved range).
fn put_lit(bytes: &[u8], s: &mut Scratch<'_>, w: &mut usize) {
    for &b in bytes {
        s.buf[*w] = b;
        *w += 1;
    }
}

/// Dispatch one builtin call. `args` are in declaration order (receiver
/// first). Scratch-producing functions append into `s`. Ids absent from this
/// build (feature off) or unknown fail closed with `BadBuiltin`.
///
/// No builtin holds a buffer of its own on the stack. This body is inlined into
/// the evaluator loop, so a local array here is paid by every evaluation — not
/// only by the call that uses it — and on an RP part running under MPU isolation
/// the whole of that loop runs on a 2 KiB process stack.
pub fn call_builtin<'a>(
    id: u16,
    args: &[Value<'a>],
    s: &mut Scratch<'_>,
) -> Result<Value<'a>, EvalError> {
    use builtin::*;
    match id {
        #[cfg(feature = "strings")]
        SIZE => Ok(Value::Int(Source::of(&args[0])?.len() as i64)),
        #[cfg(feature = "strings")]
        JSON_GET | JSON_HAS | JSON_SET_DEFAULT => {
            // Read in place: an external document is a borrowed slice and a
            // scratch one is read where it sits. Every position is taken from
            // those borrows before anything is written.
            let docs = Source::of(&args[0])?;
            let paths = Source::of(&args[1])?;
            let found = json_locate(docs.bytes(s), paths.bytes(s));
            match id {
                JSON_HAS => Ok(Value::Bool(found.is_some())),
                JSON_GET => Ok(match found {
                    Some(span) => {
                        let (i, e) = json_unquote(docs.bytes(s), span);
                        docs.window_value(i, e)
                    }
                    None => docs.window_value(0, 0),
                }),
                _ => {
                    // The default goes in only where there is nothing: a
                    // present field — whatever its value — is the caller's.
                    if found.is_some() {
                        return Ok(args[0]);
                    }
                    let raws = Source::of(&args[2])?;
                    let p = paths.bytes(s);
                    // `k` (top level) or `parent.k` (one level down, the parent
                    // an object that exists); any other path leaves the document
                    // as it is.
                    let (parent, key) = match p.iter().position(|&c| c == b'.') {
                        None => (None, 0..p.len()),
                        Some(d) if p.iter().rposition(|&c| c == b'.') == Some(d) => {
                            (Some(0..d), d + 1..p.len())
                        }
                        Some(_) => return Ok(args[0]),
                    };
                    // The key is written verbatim, so one that JSON would need
                    // escaped is not written at all: the document comes back
                    // unchanged, not malformed or carrying members the path
                    // smuggled in.
                    if p[key.clone()]
                        .iter()
                        .any(|&c| c == b'"' || c == b'\\' || c < 0x20)
                    {
                        return Ok(args[0]);
                    }
                    let d = docs.bytes(s);
                    let obj_at = match parent {
                        None => json_ws(d, 0),
                        Some(pp) => match json_locate(d, &p[pp]) {
                            Some((i, _)) => i,
                            None => return Ok(args[0]),
                        },
                    };
                    if d.get(obj_at) != Some(&b'{') {
                        return Ok(args[0]);
                    }
                    let empty = d.get(json_ws(d, obj_at + 1)) == Some(&b'}');
                    let (dl, rl) = (d.len(), raws.len());
                    // doc[..=obj_at] "key":raw [,] doc[obj_at+1..]
                    let total = dl + 1 + key.len() + 2 + rl + usize::from(!empty);
                    let off = s.reserve(total)?;
                    let mut w = off;
                    put_src(&docs, 0, obj_at + 1, s, &mut w);
                    put_lit(b"\"", s, &mut w);
                    put_src(&paths, key.start, key.end, s, &mut w);
                    put_lit(b"\":", s, &mut w);
                    put_src(&raws, 0, rl, s, &mut w);
                    if !empty {
                        put_lit(b",", s, &mut w);
                    }
                    put_src(&docs, obj_at + 1, dl, s, &mut w);
                    Ok(Value::Scratch {
                        off: off as u32,
                        len: total as u32,
                    })
                }
            }
        }
        #[cfg(feature = "strings")]
        CONCAT => {
            let a = Source::of(&args[0])?;
            let b = Source::of(&args[1])?;
            let (al, bl) = (a.len(), b.len());
            let off = s.reserve(al + bl)?;
            for i in 0..al {
                s.buf[off + i] = a.at(i, s);
            }
            for i in 0..bl {
                s.buf[off + al + i] = b.at(i, s);
            }
            Ok(Value::Scratch {
                off: off as u32,
                len: (al + bl) as u32,
            })
        }
        #[cfg(feature = "strings")]
        PART => {
            let src = Source::of(&args[0])?;
            let delim = Source::of(&args[1])?;
            // A negative index is a type error; one past the address space is
            // past the last part, on every target. `try_from` rather than `as`,
            // which would truncate on a 32-bit core.
            let want = arg_int(&args[2])?;
            if want < 0 {
                return Err(EvalError::TypeError);
            }
            let want = usize::try_from(want).unwrap_or(usize::MAX);
            let (n, dn) = (src.len(), delim.len());
            if dn == 0 {
                return Err(EvalError::TypeError);
            }
            let mut start = 0usize;
            let mut k = 0usize;
            let mut i = 0usize;
            while i + dn <= n {
                if src.window_eq(i, &delim, s) {
                    if k == want {
                        return Ok(src.window_value(start, i));
                    }
                    k += 1;
                    i += dn;
                    start = i;
                } else {
                    i += 1;
                }
            }
            Ok(if k == want {
                src.window_value(start, n)
            } else {
                src.window_value(0, 0)
            })
        }
        #[cfg(feature = "strings")]
        CONTAINS | STARTS_WITH | ENDS_WITH | INDEX_OF | LAST_INDEX_OF => {
            let h = Source::of(&args[0])?;
            let n = Source::of(&args[1])?;
            Ok(match id {
                CONTAINS => Value::Bool(find(&h, &n, s) >= 0),
                STARTS_WITH => Value::Bool(n.len() <= h.len() && h.window_eq(0, &n, s)),
                ENDS_WITH => {
                    Value::Bool(n.len() <= h.len() && h.window_eq(h.len() - n.len(), &n, s))
                }
                INDEX_OF => Value::Int(find(&h, &n, s)),
                _ => Value::Int(rfind(&h, &n, s)), // LAST_INDEX_OF
            })
        }
        #[cfg(feature = "strings")]
        CHAR_AT | SUBSTRING | SUBSTRING_RANGE | TRIM => {
            let src = Source::of(&args[0])?;
            let len = src.len();
            let (lo, hi) = match id {
                CHAR_AT => {
                    let i = checked_index(arg_int(&args[1])?, len)?;
                    if i == len {
                        return Err(EvalError::TypeError); // CEL: OOR errors
                    }
                    (i, i + 1)
                }
                SUBSTRING => (checked_index(arg_int(&args[1])?, len)?, len),
                SUBSTRING_RANGE => {
                    let a = checked_index(arg_int(&args[1])?, len)?;
                    let b = checked_index(arg_int(&args[2])?, len)?;
                    if a > b {
                        return Err(EvalError::TypeError);
                    }
                    (a, b)
                }
                _ => {
                    // TRIM
                    let mut a = 0;
                    let mut b = len;
                    while a < b && is_ascii_ws(src.at(a, s)) {
                        a += 1;
                    }
                    while b > a && is_ascii_ws(src.at(b - 1, s)) {
                        b -= 1;
                    }
                    (a, b)
                }
            };
            Ok(src.window_value(lo, hi))
        }
        #[cfg(feature = "strings")]
        REVERSE_STR => {
            // Reverse by UTF-8 code point: sequences emitted last-to-first,
            // each keeping its internal order. Boundary detection is
            // structural (continuations are 0b10xxxxxx) — no tables; invalid
            // bytes travel as 1-byte units so arbitrary input stays total.
            let src = Source::of(&args[0])?;
            let n = src.len();
            let off = s.reserve(n)?;
            let mut w = off;
            let mut end = n;
            while end > 0 {
                let mut start = end - 1;
                let mut back = 0;
                while back < 3 && start > 0 && src.at(start, s) & 0b1100_0000 == 0b1000_0000 {
                    start -= 1;
                    back += 1;
                }
                // The lead must claim exactly the continuations behind it;
                // otherwise emit the last byte alone (invalid stays inert).
                let take = if utf8_claim(src.at(start, s)) == end - start {
                    end - start
                } else {
                    1
                };
                let from = end - take;
                let mut i = 0;
                while i < take {
                    s.buf[w + i] = src.at(from + i, s);
                    i += 1;
                }
                w += take;
                end = from;
            }
            Ok(Value::Scratch {
                off: off as u32,
                len: n as u32,
            })
        }
        #[cfg(feature = "strings")]
        REVERSE_BYTES | LOWER_ASCII | UPPER_ASCII => {
            let src = Source::of(&args[0])?;
            let n = src.len();
            let off = s.reserve(n)?;
            let mut i = 0;
            while i < n {
                let b = match id {
                    REVERSE_BYTES => src.at(n - 1 - i, s),
                    LOWER_ASCII => src.at(i, s).to_ascii_lowercase(),
                    _ => src.at(i, s).to_ascii_uppercase(), // UPPER_ASCII
                };
                s.buf[off + i] = b;
                i += 1;
            }
            Ok(Value::Scratch {
                off: off as u32,
                len: n as u32,
            })
        }
        #[cfg(feature = "strings")]
        REPLACE => {
            // CEL replace-all; empty `from` is refused rather than the
            // "insert everywhere" surprise.
            let h = Source::of(&args[0])?;
            let from = Source::of(&args[1])?;
            let to = Source::of(&args[2])?;
            if from.len() == 0 {
                return Err(EvalError::TypeError);
            }
            let start = s.used;
            let mut i = 0;
            while i < h.len() {
                if h.window_eq(i, &from, s) {
                    let o = s.reserve(to.len())?;
                    let mut k = 0;
                    while k < to.len() {
                        s.buf[o + k] = to.at(k, s);
                        k += 1;
                    }
                    i += from.len();
                } else {
                    let b = h.at(i, s);
                    let o = s.reserve(1)?;
                    s.buf[o] = b;
                    i += 1;
                }
            }
            Ok(Value::Scratch {
                off: start as u32,
                len: (s.used - start) as u32,
            })
        }
        #[cfg(feature = "math")]
        MATH_GREATEST | MATH_LEAST => {
            let a = arg_int(&args[0])?;
            let b = arg_int(&args[1])?;
            Ok(Value::Int(if (a > b) == (id == MATH_GREATEST) {
                a
            } else {
                b
            }))
        }
        #[cfg(feature = "math")]
        MATH_ABS => {
            // i64::MIN has no absolute value; CEL errors on overflow.
            arg_int(&args[0])?
                .checked_abs()
                .map(Value::Int)
                .ok_or(EvalError::TypeError)
        }
        #[cfg(feature = "math")]
        MATH_SIGN => Ok(Value::Int(arg_int(&args[0])?.signum())),
        #[cfg(feature = "math")]
        BIT_AND | BIT_OR | BIT_XOR => {
            let a = arg_int(&args[0])?;
            let b = arg_int(&args[1])?;
            Ok(Value::Int(match id {
                BIT_AND => a & b,
                BIT_OR => a | b,
                _ => a ^ b, // BIT_XOR
            }))
        }
        #[cfg(feature = "math")]
        BIT_SHL | BIT_SHR => {
            let a = arg_int(&args[0])?;
            let n = arg_int(&args[1])?;
            // CEL math errors on shifts outside 0..64 rather than wrapping.
            if !(0..64).contains(&n) {
                return Err(EvalError::TypeError);
            }
            Ok(Value::Int(if id == BIT_SHL { a << n } else { a >> n }))
        }
        #[cfg(feature = "encoders")]
        B64_ENCODE => {
            let src = Source::of(&args[0])?;
            let n = src.len();
            let out_len = n.div_ceil(3) * 4;
            let off = s.reserve(out_len)?;
            let mut w = off;
            let mut i = 0;
            while i < n {
                let b0 = src.at(i, s);
                let b1 = if i + 1 < n { src.at(i + 1, s) } else { 0 };
                let b2 = if i + 2 < n { src.at(i + 2, s) } else { 0 };
                s.buf[w] = B64_ALPHABET[(b0 >> 2) as usize];
                s.buf[w + 1] = B64_ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize];
                s.buf[w + 2] = if i + 1 < n {
                    B64_ALPHABET[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize]
                } else {
                    b'='
                };
                s.buf[w + 3] = if i + 2 < n {
                    B64_ALPHABET[(b2 & 0x3f) as usize]
                } else {
                    b'='
                };
                w += 4;
                i += 3;
            }
            Ok(Value::Scratch {
                off: off as u32,
                len: out_len as u32,
            })
        }
        #[cfg(feature = "encoders")]
        B64_DECODE => {
            // Strict: canonical padding, no whitespace, no mid-stream `=`.
            // Anything else is TypeError — a codec that guesses is no codec.
            let src = Source::of(&args[0])?;
            let n = src.len();
            if n % 4 != 0 {
                return Err(EvalError::TypeError);
            }
            if n == 0 {
                return Ok(Value::Scratch {
                    off: s.used as u32,
                    len: 0,
                });
            }
            let pad = if src.at(n - 1, s) == b'=' {
                if src.at(n - 2, s) == b'=' {
                    2
                } else {
                    1
                }
            } else {
                0
            };
            let out_len = n / 4 * 3 - pad;
            let off = s.reserve(out_len)?;
            let mut w = off;
            let mut i = 0;
            while i < n {
                let last = i + 4 == n;
                let c0 = b64_val(src.at(i, s)).ok_or(EvalError::TypeError)?;
                let c1 = b64_val(src.at(i + 1, s)).ok_or(EvalError::TypeError)?;
                let (x2, x3) = (src.at(i + 2, s), src.at(i + 3, s));
                let (c2, c3) = match (x2, x3) {
                    (b'=', b'=') if last && pad == 2 => (0, 0),
                    (x, b'=') if last && pad == 1 => (b64_val(x).ok_or(EvalError::TypeError)?, 0),
                    (x, y) => (
                        b64_val(x).ok_or(EvalError::TypeError)?,
                        b64_val(y).ok_or(EvalError::TypeError)?,
                    ),
                };
                let n_out = if last { 3 - pad } else { 3 };
                if n_out >= 1 {
                    s.buf[w] = (c0 << 2) | (c1 >> 4);
                }
                if n_out >= 2 {
                    s.buf[w + 1] = (c1 << 4) | (c2 >> 2);
                }
                if n_out == 3 {
                    s.buf[w + 2] = (c2 << 6) | c3;
                }
                w += n_out;
                i += 4;
            }
            Ok(Value::Scratch {
                off: off as u32,
                len: out_len as u32,
            })
        }
        _ => Err(EvalError::BadBuiltin(id)),
    }
}
