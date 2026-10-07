// Exchange records ↔ pipeline records.
//
// A pipeline meets the exchange contract (the SDK's `contracts/exchange.rs`)
// in four roles, chosen by which of its ports a graph wires:
//
//   provider   `request_in`: each request is collected whole and becomes a
//              record; the exchange is answered once that record resolves
//              (`ack`), or by whichever node downstream writes its response.
//   answer     `response_out`: a record becomes the response to the exchange
//              its id field names.
//   requester  `request_out` + `response_in`: a record becomes a request this
//              pipeline correlates itself; the answer becomes a record.
//   relay      `request_in` + `request_out`, with `response_in` +
//              `response_out`: a request HEAD is rewritten and passed on under
//              the SAME id, and every other record of the exchange is forwarded
//              untouched each way, so the provider answers the original
//              requester and a body of any length streams. A request the relay
//              cannot pass on — or whose stages name a status — it answers
//              itself.
//   response relay  `response_in` + `response_out` alone: another node's
//              answers pass through, so several nodes answer one requester
//              through its single `response_in`.
//
// The parts of an exchange that are not its body — the id, method, target,
// headers, status and content type — map to DATA fields of the record, chosen
// per graph by a `FieldMap`. Stages read and write them like any other field,
// and a codec only ever sees a body: no program frames a transport record.
//
// Pure: no syscalls and no SDK runtime, so the harness drives the same code.
// Mounted after vm_core and pipeline_core, beside the SDK's exchange contract
// as `exchange`.

use exchange::{
    write_request_head, write_response_head, ExchangeId, RequestHead, ResponseHead, ID_LEN,
};

/// Which data fields carry the parts of an exchange that are not its body.
/// `0` leaves a part unmapped: it is neither delivered into a record nor read
/// from one, and the role's default applies.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FieldMap {
    /// The exchange id, as 14 bytes.
    pub id: u8,
    pub method: u8,
    pub target: u8,
    pub headers: u8,
    pub status: u8,
    pub content_type: u8,
}

/// Why a record could not become an exchange record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExchangeFault {
    /// The record names no exchange id where the role needs one.
    NoId,
    /// The id field is not 14 bytes.
    BadId,
    /// A mapped field holds the wrong kind of value.
    BadField,
    /// The record does not fit one exchange record.
    TooLarge,
    /// More fields than the table holds.
    TooManyFields,
}

/// Whether `n` may name a data field: `1..=239`.
pub const fn is_data_field(n: u8) -> bool {
    n >= 1 && n <= 239
}

fn push<'a>(
    fields: &mut [Field<'a>],
    n: &mut usize,
    number: u8,
    value: Value<'a>,
) -> Result<(), ExchangeFault> {
    if number == 0 {
        return Ok(());
    }
    let slot = fields.get_mut(*n).ok_or(ExchangeFault::TooManyFields)?;
    *slot = Field {
        number: number as u32,
        value,
    };
    *n += 1;
    Ok(())
}

/// Append a request's mapped parts to `fields[n..]`; returns the new count.
pub fn request_fields<'a>(
    map: &FieldMap,
    id: &'a [u8; ID_LEN],
    method: u8,
    target: &'a [u8],
    headers: &'a [u8],
    fields: &mut [Field<'a>],
    mut n: usize,
) -> Result<usize, ExchangeFault> {
    push(fields, &mut n, map.id, Value::Bytes(id))?;
    push(fields, &mut n, map.method, Value::Int(method as i64))?;
    push(fields, &mut n, map.target, Value::Bytes(target))?;
    push(fields, &mut n, map.headers, Value::Bytes(headers))?;
    Ok(n)
}

/// Append a response's mapped parts to `fields[n..]`; returns the new count.
pub fn response_fields<'a>(
    map: &FieldMap,
    status: u16,
    content_type: &'a [u8],
    headers: &'a [u8],
    fields: &mut [Field<'a>],
    mut n: usize,
) -> Result<usize, ExchangeFault> {
    push(fields, &mut n, map.status, Value::Int(status as i64))?;
    push(fields, &mut n, map.content_type, Value::Bytes(content_type))?;
    push(fields, &mut n, map.headers, Value::Bytes(headers))?;
    Ok(n)
}

fn mapped<'a>(fields: &[Field<'a>], number: u8) -> Option<Value<'a>> {
    if number == 0 {
        return None;
    }
    fields
        .iter()
        .find(|f| f.number == number as u32)
        .map(|f| f.value)
}

/// The bytes of mapped field `number`: absent (or unmapped) is empty.
fn bytes_of<'a>(fields: &[Field<'a>], number: u8) -> Result<&'a [u8], ExchangeFault> {
    match mapped(fields, number) {
        None | Some(Value::Null) => Ok(&[]),
        Some(Value::Bytes(b)) | Some(Value::Frame(b)) => Ok(b),
        Some(Value::Str(s)) => Ok(s.as_bytes()),
        Some(_) => Err(ExchangeFault::BadField),
    }
}

/// The integer in mapped field `number`, or `default` when it is absent.
fn int_of(fields: &[Field<'_>], number: u8, default: i64) -> Result<i64, ExchangeFault> {
    match mapped(fields, number) {
        None | Some(Value::Null) => Ok(default),
        Some(Value::Int(i)) => Ok(i),
        Some(Value::Uint(u)) => i64::try_from(u).map_err(|_| ExchangeFault::BadField),
        Some(_) => Err(ExchangeFault::BadField),
    }
}

/// The exchange id a record carries in the mapped id field.
pub fn record_id(map: &FieldMap, fields: &[Field<'_>]) -> Result<ExchangeId, ExchangeFault> {
    if map.id == 0 {
        return Err(ExchangeFault::NoId);
    }
    let b = match mapped(fields, map.id) {
        None | Some(Value::Null) => return Err(ExchangeFault::NoId),
        Some(Value::Bytes(b)) | Some(Value::Frame(b)) => b,
        Some(_) => return Err(ExchangeFault::BadId),
    };
    let arr: [u8; ID_LEN] = b.try_into().map_err(|_| ExchangeFault::BadId)?;
    Ok(ExchangeId(arr))
}

/// Write the request HEAD a record asks: method, target and headers from the
/// mapped fields (`default_method` when the method is unmapped or absent), the
/// body inline. `flags` carries MORE for a relayed HEAD whose body continues.
#[allow(
    clippy::too_many_arguments,
    reason = "every part of a request HEAD, passed explicitly so the core stays a pure function"
)]
pub fn request_head(
    map: &FieldMap,
    fields: &[Field<'_>],
    id: ExchangeId,
    flags: u8,
    default_method: u8,
    resp_credit: u32,
    body: &[u8],
    out: &mut [u8],
) -> Result<usize, ExchangeFault> {
    let method = int_of(fields, map.method, default_method as i64)?;
    let method = u8::try_from(method).map_err(|_| ExchangeFault::BadField)?;
    let head = RequestHead {
        id,
        flags,
        method,
        target: bytes_of(fields, map.target)?,
        headers: bytes_of(fields, map.headers)?,
        peer: &[],
        resp_credit,
        body,
    };
    write_request_head(&head, out).ok_or(ExchangeFault::TooLarge)
}

/// Write the response HEAD a record answers with: one HEAD carrying the whole
/// body, the status from the mapped field (`default_status` when absent), the
/// content type from its field or `default_content_type`.
pub fn response_head(
    map: &FieldMap,
    fields: &[Field<'_>],
    id: ExchangeId,
    default_status: u16,
    default_content_type: &[u8],
    body: &[u8],
    out: &mut [u8],
) -> Result<usize, ExchangeFault> {
    let status = int_of(fields, map.status, default_status as i64)?;
    let status = u16::try_from(status).map_err(|_| ExchangeFault::BadField)?;
    let ct = bytes_of(fields, map.content_type)?;
    let head = ResponseHead {
        id,
        flags: 0,
        status,
        content_type: if ct.is_empty() {
            default_content_type
        } else {
            ct
        },
        headers: bytes_of(fields, map.headers)?,
        body,
    };
    write_response_head(&head, out).ok_or(ExchangeFault::TooLarge)
}

// ── The requester's exchanges in flight ──────────────────────────────────

/// One exchange this pipeline asked and has not had answered.
#[derive(Clone, Copy)]
struct Slot<const CARRY: usize, const REQ: usize, const RESP: usize> {
    live: bool,
    /// Owed to the provider: lost to a LINK_DOWN, or opened while the link
    /// was down. Sent once the link is up.
    replay: bool,
    id: u64,
    carry: [u8; CARRY],
    carry_len: u16,
    /// The request as sent, kept for a replay.
    req: [u8; REQ],
    req_len: u16,
    /// The response so far: its HEAD's status and content type, and the body
    /// collected across BODY records.
    head_seen: bool,
    status: u16,
    content_type: [u8; 64],
    content_type_len: u8,
    body: [u8; RESP],
    body_len: u32,
}

impl<const CARRY: usize, const REQ: usize, const RESP: usize> Slot<CARRY, REQ, RESP> {
    const EMPTY: Self = Slot {
        live: false,
        replay: false,
        id: 0,
        carry: [0; CARRY],
        carry_len: 0,
        req: [0; REQ],
        req_len: 0,
        head_seen: false,
        status: 0,
        content_type: [0; 64],
        content_type_len: 0,
        body: [0; RESP],
        body_len: 0,
    };
}

/// What a response record did to the exchange it answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer {
    /// Progress: more of the response is to come.
    Partial,
    /// The response is whole: read it with [`Inflight::answer`], then
    /// [`Inflight::release`] the slot.
    Whole(usize),
    /// The exchange ended without an answer (an ABORT); its slot is freed.
    Aborted,
    /// The response does not fit what this pipeline holds; the slot is freed.
    TooLarge,
    /// A record for no exchange this pipeline has open.
    Unknown,
    /// LINK_DOWN: every open exchange is now marked for replay.
    LinkDown(u32),
    /// LINK_UP: replays may be sent.
    LinkUp,
}

/// A whole response.
#[derive(Clone, Copy, Debug)]
pub struct Answered<'a> {
    pub status: u16,
    pub content_type: &'a [u8],
    pub body: &'a [u8],
    /// The carry the request's record held, restored on the answer.
    pub carry: &'a [u8],
}

/// The requester's exchanges in flight: up to `SLOTS`, each keeping its
/// record's carry (up to `CARRY` bytes), its request as sent (up to `REQ`) for
/// a replay, and its response body as it arrives (up to `RESP`).
pub struct Inflight<const SLOTS: usize, const CARRY: usize, const REQ: usize, const RESP: usize> {
    slots: [Slot<CARRY, REQ, RESP>; SLOTS],
    next_id: u64,
}

impl<const SLOTS: usize, const CARRY: usize, const REQ: usize, const RESP: usize>
    Inflight<SLOTS, CARRY, REQ, RESP>
{
    pub const fn new() -> Self {
        Inflight {
            slots: [Slot::EMPTY; SLOTS],
            next_id: 1,
        }
    }

    /// Exchanges open.
    pub fn open_count(&self) -> u32 {
        self.slots.iter().filter(|s| s.live).count() as u32
    }

    /// Whether another exchange can be opened.
    pub fn has_room(&self) -> bool {
        self.slots.iter().any(|s| !s.live)
    }

    /// Reserve a slot for a new exchange and return its id. The request bytes
    /// are written with [`Inflight::request_buf`] and committed with
    /// [`Inflight::commit`]. `None` when the window is full or the carry does
    /// not fit — refused, never truncated, since a clipped context rejoins the
    /// wrong request.
    pub fn reserve(&mut self, carry: &[u8]) -> Option<(usize, ExchangeId)> {
        if carry.len() > CARRY {
            return None;
        }
        let at = self.slots.iter().position(|s| !s.live)?;
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);
        let s = &mut self.slots[at];
        *s = Slot::EMPTY;
        s.id = id;
        s.carry[..carry.len()].copy_from_slice(carry);
        s.carry_len = carry.len() as u16;
        Some((at, ExchangeId::from_u64(id)))
    }

    /// Where the reserved slot's request is written.
    pub fn request_buf(&mut self, at: usize) -> &mut [u8] {
        match self.slots.get_mut(at) {
            Some(s) => &mut s.req,
            None => &mut [],
        }
    }

    /// The reserved slot's request is in place and `len` bytes long: the
    /// exchange is open. With `held` the request is owed rather than sent —
    /// the link is down, and a provider drops what arrives before its LINK_UP
    /// — and goes with the replays once the link is back.
    pub fn commit(&mut self, at: usize, len: usize, held: bool) {
        if let Some(s) = self.slots.get_mut(at) {
            if len <= REQ {
                s.req_len = len as u16;
                s.live = true;
                s.replay = held;
            }
        }
    }

    /// Give back a reserved slot whose request was never sent.
    pub fn cancel(&mut self, at: usize) {
        if let Some(s) = self.slots.get_mut(at) {
            *s = Slot::EMPTY;
        }
    }

    /// The request owed for the next exchange a LINK_DOWN lost or held, if any.
    pub fn next_replay(&self) -> Option<(usize, &[u8])> {
        let at = self.slots.iter().position(|s| s.live && s.replay)?;
        let s = &self.slots[at];
        Some((at, &s.req[..s.req_len as usize]))
    }

    /// The replay of slot `at` is away.
    pub fn replayed(&mut self, at: usize) {
        if let Some(s) = self.slots.get_mut(at) {
            s.replay = false;
        }
    }

    fn find(&self, id: &ExchangeId) -> Option<usize> {
        let n = id.as_u64();
        if id.0[8..].iter().any(|b| *b != 0) {
            return None;
        }
        self.slots.iter().position(|s| s.live && s.id == n)
    }

    /// Take one response-direction record.
    pub fn accept(&mut self, record: &[u8]) -> Answer {
        let Some(rec) = exchange::parse_response(record) else {
            return Answer::Unknown;
        };
        match rec {
            exchange::Record::Link { state } => {
                if state == exchange::link::DOWN {
                    let mut lost = 0u32;
                    for s in self.slots.iter_mut().filter(|s| s.live) {
                        // The answer so far is from the attempt that was lost.
                        s.replay = true;
                        s.head_seen = false;
                        s.body_len = 0;
                        lost += 1;
                    }
                    Answer::LinkDown(lost)
                } else {
                    Answer::LinkUp
                }
            }
            exchange::Record::Head(h) => {
                let Some(at) = self.find(&h.id) else {
                    return Answer::Unknown;
                };
                let s = &mut self.slots[at];
                s.head_seen = true;
                s.status = h.status;
                let ct = h.content_type;
                if ct.len() > s.content_type.len() {
                    *s = Slot::EMPTY;
                    return Answer::TooLarge;
                }
                s.content_type[..ct.len()].copy_from_slice(ct);
                s.content_type_len = ct.len() as u8;
                s.body_len = 0;
                self.extend(at, h.flags, h.body)
            }
            exchange::Record::Body { id, flags, data } => {
                let Some(at) = self.find(&id) else {
                    return Answer::Unknown;
                };
                if !self.slots[at].head_seen {
                    return Answer::Unknown;
                }
                self.extend(at, flags, data)
            }
            exchange::Record::Abort { id, .. } => match self.find(&id) {
                Some(at) => {
                    self.slots[at] = Slot::EMPTY;
                    Answer::Aborted
                }
                None => Answer::Unknown,
            },
            // A request-body credit: this pipeline sends its whole request in
            // the HEAD, so there is nothing to release.
            exchange::Record::Credit { .. } | exchange::Record::Datagram { .. } => Answer::Partial,
        }
    }

    fn extend(&mut self, at: usize, flags: u8, data: &[u8]) -> Answer {
        let s = &mut self.slots[at];
        let have = s.body_len as usize;
        let Some(want) = have.checked_add(data.len()).filter(|w| *w <= RESP) else {
            *s = Slot::EMPTY;
            return Answer::TooLarge;
        };
        s.body[have..want].copy_from_slice(data);
        s.body_len = want as u32;
        if flags & exchange::flag::MORE == 0 {
            Answer::Whole(at)
        } else {
            Answer::Partial
        }
    }

    /// The whole response in slot `at`.
    pub fn answer(&self, at: usize) -> Option<Answered<'_>> {
        let s = self.slots.get(at)?;
        if !s.live || !s.head_seen {
            return None;
        }
        Some(Answered {
            status: s.status,
            content_type: &s.content_type[..s.content_type_len as usize],
            body: &s.body[..s.body_len as usize],
            carry: &s.carry[..s.carry_len as usize],
        })
    }

    /// Free slot `at`, once its answer is delivered.
    pub fn release(&mut self, at: usize) {
        if let Some(s) = self.slots.get_mut(at) {
            *s = Slot::EMPTY;
        }
    }
}

impl<const SLOTS: usize, const CARRY: usize, const REQ: usize, const RESP: usize> Default
    for Inflight<SLOTS, CARRY, REQ, RESP>
{
    fn default() -> Self {
        Self::new()
    }
}
