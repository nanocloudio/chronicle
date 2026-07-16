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
// Each entry is gated by its extension's cfg feature (RFC module_variants);
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
        | UPPER_ASCII | REPLACE => true,
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
        | MATH_GREATEST | MATH_LEAST | BIT_AND | BIT_OR | BIT_XOR | BIT_SHL | BIT_SHR => 2,
        SUBSTRING_RANGE | REPLACE => 3,
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
fn checked_index(i: i64, len: usize) -> Result<usize, EvalError> {
    if i < 0 || i as usize > len {
        return Err(EvalError::TypeError);
    }
    Ok(i as usize)
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

/// Dispatch one builtin call. `args` are in declaration order (receiver
/// first). Scratch-producing functions append into `s`. Ids absent from this
/// build (feature off) or unknown fail closed with `BadBuiltin`.
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
