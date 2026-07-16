// Bounded, no_std, no-alloc BYTE-SERIALIZATION VM. Like the other `*_core.rs`
// files it has NO inner attributes and NO test module, so it is `include!`d
// verbatim by both this crate and the on-device modules — one source of truth.
//
// This turns "build a wire request from a record" into ordinary bytecode: an
// encoder is a Transformation whose result is a byte buffer, not a message. The
// value machinery (LOAD_PARAM / GET_FIELD / PUSH_* / arithmetic) is shared with
// `core.rs`; these opcodes append bytes to an output buffer and frame regions
// (length prefixes, varints, CRC-32C) so protocols like RESP, the Postgres
// simple-query stream, MQTT packets, and Kafka RecordBatches are all expressible
// with no protocol-specific module and no external payload builder.
//
// Region model: emitting always appends at the tail, which is the innermost open
// region. Closing a region prepends its frame (len / varint / crc) by shifting
// the region's bytes right and writing the prefix — so nested framing composes.

/// Byte-serialization opcodes (0x60+, disjoint from the value/message opcodes in
/// `core.rs::op`).
// Decimal ASCII rendering, used by the `LEN`/`VAL` integer paths below and by
// length-prefix regions. Lived in the retired `resp_core` (Redis reply framing,
// now lattice's `redis_client`); it is pure integer formatting with no protocol
// in it, so it moved to its only remaining consumer rather than being deleted.
/// Write `v` as decimal ASCII into `out`; returns the length, or `None` if `out`
/// is too small. No allocation, no panic path (freestanding-module safe).
pub fn itoa(v: i64, out: &mut [u8]) -> Option<usize> {
    let neg = v < 0;
    let mut u: u64 = if neg {
        (v as i128).unsigned_abs() as u64
    } else {
        v as u64
    };
    let mut tmp = [0u8; 20];
    let mut n = 0usize;
    if u == 0 {
        tmp[0] = b'0';
        n = 1;
    } else {
        while u > 0 {
            tmp[n] = b'0' + (u % 10) as u8;
            u /= 10;
            n += 1;
        }
    }
    let total = n + usize::from(neg);
    if total > out.len() {
        return None;
    }
    let mut i = 0;
    if neg {
        out[0] = b'-';
        i = 1;
    }
    let mut k = 0;
    while k < n {
        out[i + k] = tmp[n - 1 - k];
        k += 1;
    }
    Some(total)
}

pub mod ser {
    pub const LIT: u8 = 0x60; // len:u16 LE, bytes — append literal
    pub const VAL: u8 = 0x61; // pop value → append (bytes raw / int decimal / bool "0"/"1")
    pub const INT: u8 = 0x62; // width:u8, endian:u8(0=BE,1=LE) — pop int → append binary
    pub const VARINT: u8 = 0x63; // pop int → append zig-zag varint
    pub const LEN: u8 = 0x64; // pop bytes/str → push Int(byte length)
    pub const RGN_BEGIN: u8 = 0x65; // open a region
    pub const RGN_END: u8 = 0x66; // close: merge into parent (no prefix)
    pub const RGN_LEN: u8 = 0x67; // width:u8, endian:u8, delta:i8 — prepend length
    pub const RGN_VARINT: u8 = 0x68; // prepend varint(len)
    pub const RGN_CRC: u8 = 0x69; // prepend crc32c (4 bytes BE)
    pub const FINISH: u8 = 0x6A; // terminator: result = the serialized bytes
    pub const RGN_DECLEN: u8 = 0x6B; // prepend "<decimal len>\r\n" (RESP bulk header)
    pub const RGN_ZIGVARINT: u8 = 0x6C; // prepend zig-zag (signed) varint(len) (Kafka)
}

const SER_STACK: usize = 32;
const MAX_REGIONS: usize = 12;

/// CRC-32C (Castagnoli, reflected) — used by `ser::RGN_CRC` (Kafka RecordBatch).
fn ser_crc32c(data: &[u8]) -> u32 {
    let mut crc: u32 = !0;
    for &b in data {
        crc ^= b as u32;
        let mut i = 0;
        while i < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
            i += 1;
        }
    }
    !crc
}

/// Execute a byte-serialization program, appending into `out`. Returns the final
/// length. Shares the value stack semantics with `eval_full`.
pub fn eval_bytes<'a>(
    code: &'a [u8],
    params: &'a [Message<'a>],
    out: &mut [u8],
    max_cost: u64,
) -> Result<usize, EvalError> {
    let mut stack: [Value<'a>; SER_STACK] = [Value::Null; SER_STACK];
    let mut sp: usize = 0;
    let mut regions: [usize; MAX_REGIONS] = [0; MAX_REGIONS];
    let mut rsp: usize = 0;
    let mut end: usize = 0;
    let mut pc: usize = 0;
    let mut cost: u64 = 0;

    macro_rules! push {
        ($v:expr) => {{
            if sp >= SER_STACK {
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
    macro_rules! put {
        ($bytes:expr) => {{
            let b = $bytes;
            if end + b.len() > out.len() {
                return Err(EvalError::BuildOverflow);
            }
            out[end..end + b.len()].copy_from_slice(b);
            end += b.len();
        }};
    }
    // Prepend `plen` bytes at region start `s`: shift [s..end] right by `plen`
    // (backward copy for overlap; manual indexing so a freestanding module links
    // no formatted-panic path, unlike `copy_within`).
    macro_rules! shift_for_prefix {
        ($s:expr, $plen:expr) => {{
            let s = $s;
            let plen = $plen;
            if end + plen > out.len() {
                return Err(EvalError::BuildOverflow);
            }
            let mut k = end;
            while k > s {
                k -= 1;
                out[k + plen] = out[k];
            }
            end += plen;
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

        // Shared value-load and arithmetic ops (see core.rs).
        if let Some(res) = load_op(opcode, code, &mut pc, params, &mut stack, &mut sp) {
            res?;
            continue;
        }
        if let Some(res) = arith_op(opcode, code, &mut pc, &mut stack, &mut sp) {
            res?;
            continue;
        }

        match opcode {
            // ---- byte serialization ----
            ser::LEN => {
                let n = match pop!() {
                    Value::Bytes(b) => b.len() as i64,
                    Value::Str(s) => s.len() as i64,
                    _ => return Err(EvalError::TypeError),
                };
                push!(Value::Int(n));
            }
            ser::LIT => {
                let b = code.get(pc..pc + 2).ok_or(EvalError::Truncated)?;
                let len = u16::from_le_bytes([b[0], b[1]]) as usize;
                pc += 2;
                let bytes = code.get(pc..pc + len).ok_or(EvalError::Truncated)?;
                pc += len;
                put!(bytes);
            }
            ser::VAL => match pop!() {
                Value::Bytes(b) => put!(b),
                Value::Str(s) => put!(s.as_bytes()),
                Value::Int(i) => {
                    let mut tmp = [0u8; 20];
                    let n = itoa(i, &mut tmp).ok_or(EvalError::BuildOverflow)?;
                    put!(&tmp[..n]);
                }
                Value::Uint(u) => {
                    let mut tmp = [0u8; 20];
                    let n = itoa(u as i64, &mut tmp).ok_or(EvalError::BuildOverflow)?;
                    put!(&tmp[..n]);
                }
                Value::Bool(bl) => put!(if bl { b"1".as_slice() } else { b"0".as_slice() }),
                _ => return Err(EvalError::TypeError),
            },
            ser::INT => {
                let width = *code.get(pc).ok_or(EvalError::Truncated)? as usize;
                let endian = *code.get(pc + 1).ok_or(EvalError::Truncated)?;
                pc += 2;
                let v = as_int(pop!())?;
                if width == 0 || width > 8 {
                    return Err(EvalError::TypeError);
                }
                let le = v.to_le_bytes();
                let mut buf = [0u8; 8];
                if endian == 1 {
                    buf[..width].copy_from_slice(&le[..width]); // little-endian: low bytes
                } else {
                    // big-endian: high `width` bytes of the value, MSB first
                    for k in 0..width {
                        buf[k] = le[width - 1 - k];
                    }
                }
                put!(&buf[..width]);
            }
            ser::VARINT => {
                let v = as_int(pop!())?;
                let mut u = ((v << 1) ^ (v >> 63)) as u64;
                loop {
                    let mut byte = (u & 0x7f) as u8;
                    u >>= 7;
                    if u != 0 {
                        byte |= 0x80;
                    }
                    put!(&[byte]);
                    if u == 0 {
                        break;
                    }
                }
            }
            ser::RGN_BEGIN => {
                if rsp >= MAX_REGIONS {
                    return Err(EvalError::StackOverflow);
                }
                regions[rsp] = end;
                rsp += 1;
            }
            ser::RGN_END => {
                if rsp == 0 {
                    return Err(EvalError::StackUnderflow);
                }
                rsp -= 1;
            }
            ser::RGN_LEN => {
                let width = *code.get(pc).ok_or(EvalError::Truncated)? as usize;
                let endian = *code.get(pc + 1).ok_or(EvalError::Truncated)?;
                let delta = *code.get(pc + 2).ok_or(EvalError::Truncated)? as i8 as i64;
                pc += 3;
                if width == 0 || width > 8 {
                    return Err(EvalError::TypeError); // validate before mutating the buffer
                }
                if rsp == 0 {
                    return Err(EvalError::StackUnderflow);
                }
                rsp -= 1;
                let s = regions[rsp];
                let len = (end - s) as i64 + delta;
                shift_for_prefix!(s, width);
                let le = len.to_le_bytes();
                if endian == 1 {
                    out[s..s + width].copy_from_slice(&le[..width]);
                } else {
                    for k in 0..width {
                        out[s + k] = le[width - 1 - k];
                    }
                }
            }
            ser::RGN_VARINT | ser::RGN_ZIGVARINT => {
                if rsp == 0 {
                    return Err(EvalError::StackUnderflow);
                }
                rsp -= 1;
                let s = regions[rsp];
                let raw = (end - s) as i64;
                // Unsigned varint (MQTT remaining length) or zig-zag signed varint
                // (Kafka record/key/value lengths).
                let mut u = if opcode == ser::RGN_ZIGVARINT {
                    ((raw << 1) ^ (raw >> 63)) as u64
                } else {
                    raw as u64
                };
                let mut vb = [0u8; 10];
                let mut n = 0;
                loop {
                    let mut byte = (u & 0x7f) as u8;
                    u >>= 7;
                    if u != 0 {
                        byte |= 0x80;
                    }
                    vb[n] = byte;
                    n += 1;
                    if u == 0 {
                        break;
                    }
                }
                shift_for_prefix!(s, n);
                out[s..s + n].copy_from_slice(&vb[..n]);
            }
            ser::RGN_CRC => {
                if rsp == 0 {
                    return Err(EvalError::StackUnderflow);
                }
                rsp -= 1;
                let s = regions[rsp];
                let crc = ser_crc32c(&out[s..end]);
                shift_for_prefix!(s, 4);
                out[s..s + 4].copy_from_slice(&crc.to_be_bytes());
            }
            ser::RGN_DECLEN => {
                if rsp == 0 {
                    return Err(EvalError::StackUnderflow);
                }
                rsp -= 1;
                let s = regions[rsp];
                let mut tmp = [0u8; 24];
                let dl = itoa((end - s) as i64, &mut tmp).ok_or(EvalError::BuildOverflow)?;
                shift_for_prefix!(s, dl + 2);
                let mut i = 0;
                while i < dl {
                    out[s + i] = tmp[i];
                    i += 1;
                }
                out[s + dl] = b'\r';
                out[s + dl + 1] = b'\n';
            }
            ser::FINISH => return Ok(end),
            other => return Err(EvalError::BadOpcode(other)),
        }
    }
}
