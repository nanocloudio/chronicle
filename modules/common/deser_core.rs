// Bounded, no_std, no-alloc BYTE-DESERIALIZATION VM — the mirror of ser_core.rs.
// `include!`d by both the crate and the modules. Turns "parse a reply into a
// record" into ordinary bytecode: read opcodes advance a cursor over an input
// buffer and push values; the existing message-construction opcodes (SET_FIELD /
// FINISH_MSG from core.rs) assemble a record. So decoding a Redis bulk reply, a
// Postgres row, or a Kafka response is a Transformation, symmetric with encoding
// — no protocol-specific module.

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
    pub const PBFIELD: u8 = 0x7A; // field:u32 LE — pop a protobuf message (Bytes), scan for that field number, push its value (len-delimited → Bytes, varint/fixed → Int, absent → Null)
    /// `delim:u8` — like [`UNTIL`], but TOLERANT of exhaustion.
    ///
    /// `UNTIL` fails with `Truncated` when the delimiter is not there, which
    /// makes a program that reads N delimited fields a program that only works
    /// on input with exactly N of them. That is why splitting a VARIABLE-ARITY
    /// sequence — a URL path, a CSV row, a header block, a SIP request line —
    /// has needed a domain module rather than a decode program: not the absence
    /// of iteration, but the absence of a reader that can run out.
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
    /// `len:u16 LE, path` — a JSON document's value at a dotted path (object
    /// keys; decimal array indices): a string's content without its quotes, a
    /// number/bool/null as written, an object or array as its bytes; EMPTY
    /// when absent or not JSON.
    pub const JSON: u8 = 0x80;
    /// Pop b, pop a — push a when it is non-empty, else b (a path's name, else
    /// the body's).
    pub const COALESCE: u8 = 0x81;
    /// `n:u8` — pop n values (bytes, text or an integer as decimal), push their
    /// concatenation, first-pushed first. Built in the caller's SCRATCH (see
    /// [`eval_decode_scratch`]); `ScratchOverflow` when it does not fit.
    pub const CAT: u8 = 0x82;
    /// Pop a value, push it as lowercase hex (in scratch).
    pub const HEX: u8 = 0x83;
    /// `n:u16 LE` — pop a value, push it without its first n bytes; EMPTY when
    /// shorter.
    pub const DROP: u8 = 0x84;
    /// Pop cond, pop value — push value when cond is non-empty, else EMPTY (a
    /// derived value that must vanish with the thing it was derived from).
    pub const GATE: u8 = 0x85;
}

/// Bytes of a value a reader can look inside.
fn rd_bytes<'a>(v: Value<'a>) -> Result<&'a [u8], EvalError> {
    match v {
        Value::Bytes(b) => Ok(b),
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

/// Read a little-endian fixed-width integer (protobuf wire types 1 and 5).
fn pb_fixed(buf: &[u8], pos: usize, width: usize) -> Option<(i64, usize)> {
    let bytes = buf.get(pos..pos + width)?;
    let mut v: i64 = 0;
    let mut k = width;
    while k > 0 {
        k -= 1;
        v = (v << 8) | bytes[k] as i64;
    }
    Some((v, pos + width))
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
/// than borrow one (`CAT`, `HEX`). Each built value takes the front of what is
/// left, so it lives as long as the input the others borrow — the record is
/// built without an allocation either way. A program using neither needs none.
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
            let slice = input.get(pos..pos + n).ok_or(EvalError::Truncated)?;
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

        // Shared immediate/arithmetic ops (see core.rs). The deserializer has no
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
            rd::BEFORE | rd::AFTER | rd::HDR | rd::JSON => {
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
                    rd::HDR => rd_header(v, arg),
                    _ => json_value(v, arg),
                };
                push!(Value::Bytes(out));
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
                let mut need = 0usize;
                for &v in &stack[sp - n..sp] {
                    need += match v {
                        Value::Int(v) => {
                            let mut d = [0u8; 20];
                            rd_dec_digits(v, &mut d)
                        }
                        other => rd_bytes(other)?.len() * if opcode == rd::HEX { 2 } else { 1 },
                    };
                }
                if need > free.len() {
                    return Err(EvalError::ScratchOverflow);
                }
                let (head, tail) = core::mem::take(&mut free).split_at_mut(need);
                free = tail;
                let mut w = 0usize;
                for &v in &stack[sp - n..sp] {
                    match v {
                        Value::Int(v) => {
                            let mut d = [0u8; 20];
                            let dl = rd_dec_digits(v, &mut d);
                            head[w..w + dl].copy_from_slice(&d[..dl]);
                            w += dl;
                        }
                        other => {
                            let b = rd_bytes(other)?;
                            if opcode == rd::HEX {
                                const HX: &[u8; 16] = b"0123456789abcdef";
                                for &c in b {
                                    head[w] = HX[(c >> 4) as usize];
                                    head[w + 1] = HX[(c & 0x0f) as usize];
                                    w += 2;
                                }
                            } else {
                                head[w..w + b.len()].copy_from_slice(b);
                                w += b.len();
                            }
                        }
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
                let n = as_int(pop!())?;
                if n < 0 {
                    return Err(EvalError::TypeError);
                }
                push!(Value::Bytes(take!(n as usize)));
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
                        if body.len() < 5 {
                            return Err(EvalError::Truncated);
                        }
                        let mlen = ((body[1] as usize) << 24)
                            | ((body[2] as usize) << 16)
                            | ((body[3] as usize) << 8)
                            | body[4] as usize;
                        let msg = body.get(5..5 + mlen).ok_or(EvalError::Truncated)?;
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
                // Scan the protobuf message for the first field == `target`.
                let mut mp = 0usize;
                let mut result = Value::Null;
                while mp < msg.len() {
                    let (tag, next) = pb_varint(msg, mp).ok_or(EvalError::Truncated)?;
                    mp = next;
                    let field = (tag >> 3) as u32;
                    let wire = (tag & 0x07) as u8;
                    let hit = field == target;
                    match wire {
                        0 => {
                            let (v, n) = pb_varint(msg, mp).ok_or(EvalError::Truncated)?;
                            mp = n;
                            if hit {
                                result = Value::Int(v as i64);
                                break;
                            }
                        }
                        1 => {
                            let (v, n) = pb_fixed(msg, mp, 8).ok_or(EvalError::Truncated)?;
                            mp = n;
                            if hit {
                                result = Value::Int(v);
                                break;
                            }
                        }
                        2 => {
                            let (len, n) = pb_varint(msg, mp).ok_or(EvalError::Truncated)?;
                            let end = n + len as usize;
                            let val = msg.get(n..end).ok_or(EvalError::Truncated)?;
                            mp = end;
                            if hit {
                                result = Value::Bytes(val);
                                break;
                            }
                        }
                        5 => {
                            let (v, n) = pb_fixed(msg, mp, 4).ok_or(EvalError::Truncated)?;
                            mp = n;
                            if hit {
                                result = Value::Int(v);
                                break;
                            }
                        }
                        _ => return Err(EvalError::TypeError), // groups (3/4) unsupported
                    }
                }
                push!(result);
            }
            rd::DECINT => {
                let neg = input.get(pos) == Some(&b'-');
                if neg {
                    pos += 1;
                }
                let mut v: i64 = 0;
                let mut any = false;
                while let Some(&c) = input.get(pos) {
                    if !c.is_ascii_digit() {
                        break;
                    }
                    v = v.wrapping_mul(10).wrapping_add((c - b'0') as i64);
                    pos += 1;
                    any = true;
                }
                if !any {
                    return Err(EvalError::TypeError);
                }
                push!(Value::Int(if neg { -v } else { v }));
            }
            other => return Err(EvalError::BadOpcode(other)),
        }
    }
}
