//! Pipeline executor — Fluxor `.fmod` app module.
//!
//! PARAM-DRIVEN: the stage table is NOT baked — it arrives as an `ir_stages`
//! module param (hex-encoded), lowered at load into a serialized
//! `[nstages]` then per stage `[route:u8][cost:u32][len:u16][code]` container. So
//! one pipeline binary runs ANY Pipeline: the compiler emits each
//! stage's bytecode, a config packs the container, and this module threads a
//! record frame through the stages, serializing each stage's constructed message
//! as the next stage's input.
//!
//! Records arrive on `record_in` and leave on `result_out`. Where a graph wires
//! the exchange ports, the same stages run between exchange records instead, in
//! the roles `exchange_core.rs` describes — provider (`request_in`), answer
//! (`response_out` with `answer`), requester (`request_out` + `response_in`),
//! and relay in either direction. The parts of an exchange other than its body
//! map to data fields named by the `*_field` params, so a codec only ever reads
//! or writes a body.
//!
//! The staged executor + container codec live in `pipeline_core.rs`, which this
//! module and the test harness (`tests/harness/tests/pipeline.rs`) both
//! `include!`, so the tests run the code that ships.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    reason = "the fluxor SDK + shared cores are include!'d wholesale; each module consumes only a subset"
)]

use core::ffi::c_void;

#[allow(
    unused_imports,
    dead_code,
    reason = "shared SDK surface across modules"
)]
#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

// The exchange contract: one definition shared with every requester and
// provider in the workspace.
#[allow(dead_code, reason = "this module consumes a subset of the contract")]
#[rustfmt::skip]
#[path = "../../../target/fluxor/fluxor-abi/sdk/contracts/exchange.rs"]
mod exchange;
use exchange::{
    flag, kind, record_kind, Collector, ExchangeId, Refuse, KEY_MAX, PAYLOAD_MAX, RECORD_MAX,
};

/// A decode program's scratch arena (values it builds rather than borrows):
/// room for three whole-record documents at the full tier, so a decoder can
/// apply a policy's defaults (`rd::JSONDEF`, each a new copy) to a body of
/// the record ceiling; keys and hex (`CAT`, `HEX`) at the tiny tier.
const DEC_SCRATCH: usize = if TINY { 1024 } else { 3 * REC_BUF };

/// Arena class: an MCU, but not the smallest. Exchange tables scale with it.
const SMALL: bool = abi::config::kernel::STATE_ARENA_SIZE <= 512 * 1024;
/// One exchange record as read or written: the contract's whole record, or
/// what a tiny target's ports declare.
const XREC: usize = if TINY { 1024 } else { RECORD_MAX };
/// Requests collected at once as a provider.
const XSLOTS: usize = if SMALL { 1 } else { 4 };
/// A collected request's header block.
const XHDRS: usize = if TINY { 256 } else { 2048 };
/// The largest carry (field 254) a request keeps across its exchange, and the
/// largest request target collected.
const KEY_BUF: usize = if TINY { 64 } else { KEY_MAX };
/// The largest request body collected and the largest answer body assembled:
/// the contract's whole payload, or one record on a tiny target.
const XBODY: usize = if TINY { REC_BUF } else { PAYLOAD_MAX };
/// Requests in flight as a requester. The window is what applies
/// BACKPRESSURE: at the limit the pipeline admits nothing until an answer
/// frees a slot, so it never runs ahead of a provider that has not answered.
const WINDOW: usize = if TINY {
    1
} else if SMALL {
    2
} else {
    8
};
/// The reply decoder's program buffers — absent on a tiny target, which
/// takes answers whose body is already a record frame.
const RDEC_HEX_BUF: usize = if TINY { 0 } else { HEX_BUF };
const RDEC_PROG_BUF: usize = if TINY { 0 } else { PROG_BUF };
/// A content type this pipeline answers with by default.
const CT_BUF: usize = 64;

/// The admitted record reached its one defined output.
unsafe fn record_succeeded(s: &mut ModuleState, sys: &SyscallTable) {
    s.acct.input_succeeded();
    resolve_request(s, sys, true);
}

/// The admitted record reached a terminal failure; see [`record_succeeded`].
unsafe fn record_failed(s: &mut ModuleState, sys: &SyscallTable) {
    s.acct.input_failed();
    resolve_request(s, sys, false);
}

/// The exchange whose record just resolved. A request whose record failed is
/// answered here, 500, so its requester is never left waiting. Otherwise a
/// collected request is answered here with its status (`ack`), already
/// answered by its own record (`answer`), or left for the node downstream that
/// answers it; either way its slot is free.
unsafe fn resolve_request(s: &mut ModuleState, sys: &SyscallTable, ok: bool) {
    if s.relay_open {
        s.relay_open = false;
        if !ok {
            s.requests_refused = s.requests_refused.wrapping_add(1);
            let id = s.relay_id;
            respond_status(s, sys, &id, exchange::status::FAILED);
        }
    }
    if s.in_slot < 0 {
        return;
    }
    if !ok {
        s.requests_refused = s.requests_refused.wrapping_add(1);
        let id = s.in_id;
        respond_status(s, sys, &id, exchange::status::FAILED);
    } else if s.ack && !s.in_answered {
        s.answered = s.answered.wrapping_add(1);
        let id = s.in_id;
        respond_status(s, sys, &id, exchange::status::OK);
    }
    s.in_answered = false;
    s.collector.release(s.in_slot as usize);
    s.in_slot = -1;
}

/// Answer an exchange with a status and no body, on `response_out`.
unsafe fn respond_status(s: &mut ModuleState, sys: &SyscallTable, id: &ExchangeId, status: u16) {
    if let Some(n) = exchange::write_response(id, status, b"", b"", &mut s.resp_buf) {
        s.resp_box.send(sys, s.resp_out_chan, &s.resp_buf, n);
    }
}

/// Whether the result record in `out_buf` names a status in the mapped status
/// field: a request's stages refusing it, by saying how.
unsafe fn names_status(s: &ModuleState, out_len: usize) -> bool {
    let frame = core::slice::from_raw_parts(s.out_buf.as_ptr(), out_len);
    s.map.status != 0 && frame_field_bytes(frame, s.map.status).is_some()
}

/// The bytes of field `number` in a record frame, if the frame carries it —
/// a raw scan, so no decode of the whole frame just to read one field.
fn frame_field_bytes(frame: &[u8], number: u8) -> Option<&[u8]> {
    let count = *frame.first()? as usize;
    let mut p = 1usize;
    for _ in 0..count {
        let h = frame.get(p..p + 4)?;
        let len = u16::from_le_bytes([*h.get(2)?, *h.get(3)?]) as usize;
        let body = frame.get(p + 4..p + 4 + len)?;
        if *h.first()? == number {
            return Some(body);
        }
        p += 4 + len;
    }
    None
}

/// Whether a channel has a record to read.
unsafe fn readable(sys: &SyscallTable, ch: i32) -> bool {
    if ch < 0 {
        return false;
    }
    let poll = (sys.channel_poll)(ch, 0x01);
    poll > 0 && (poll as u32 & 0x01) != 0
}

/// Read one exchange record into `xrec`. `0` when none is waiting.
unsafe fn read_record(s: &mut ModuleState, sys: &SyscallTable, ch: i32) -> usize {
    if !readable(sys, ch) {
        return 0;
    }
    let n = (sys.channel_read)(ch, s.xrec.as_mut_ptr(), XREC);
    if n <= 0 {
        0
    } else {
        n as usize
    }
}

/// The requester's answers: take one response record. A whole answer becomes
/// a record on `result_out` — its body through `reply_decode` (or itself one
/// record frame), the request's carry restored at 254, the status at 253 and at
/// the mapped fields. LINK_DOWN marks every open exchange for a replay, LINK_UP
/// lets the replays go.
///
/// Never inlined, like every path here that holds a field table: inlined into
/// `module_step`, its tables would sit on the stack under every stage run.
#[inline(never)]
unsafe fn requester_step(s: &mut ModuleState, sys: &SyscallTable) {
    // A retained answer record goes first; nothing else is taken meanwhile.
    if !s.reply_pending.is_empty() {
        let plen = s.reply_pending.len as u32;
        let outch = SysChan::new(sys, s.out_chan);
        match s.reply_pending.drain(&outch, &s.reply_buf) {
            Staged::Delivered => {
                s.acct.output_drained(plen);
                s.acct.input_succeeded();
            }
            Staged::Pending => {}
            Staged::Failed(_) => {
                s.reply_pending = Pending { off: 0, len: 0 };
                s.acct.output_failed_pending(plen);
                s.acct.input_failed();
            }
        }
        return;
    }
    let n = read_record(s, sys, s.resp_in_chan);
    if n == 0 {
        return;
    }
    let rec = core::slice::from_raw_parts(s.xrec.as_ptr(), n);
    let at = match s.inflight.accept(rec) {
        Answer::Whole(at) => at,
        Answer::LinkDown(lost) => {
            s.link_downs = s.link_downs.wrapping_add(1);
            s.link_up = false;
            let _ = lost;
            return;
        }
        Answer::LinkUp => {
            s.link_up = true;
            return;
        }
        Answer::Partial => return,
        Answer::Aborted | Answer::TooLarge | Answer::Unknown => {
            s.answers_failed = s.answers_failed.wrapping_add(1);
            return;
        }
    };
    deliver_answer(s, sys, at);
    s.inflight.release(at);
}

/// Append `f` at `fields[*n]`. False when the table is full.
fn push_field<'a>(fields: &mut [Field<'a>], n: &mut usize, f: Field<'a>) -> bool {
    match fields.get_mut(*n) {
        Some(dst) => {
            *dst = f;
            *n += 1;
            true
        }
        None => false,
    }
}

/// Turn the whole answer in in-flight slot `at` into a record on `result_out`.
#[inline(never)]
unsafe fn deliver_answer(s: &mut ModuleState, sys: &SyscallTable, at: usize) {
    // Read through a raw place: the answer borrows the in-flight table while
    // the counters beside it change.
    let Some(ans) = (*core::ptr::addr_of!(s.inflight)).answer(at) else {
        return;
    };
    if ans.status >= 400 {
        s.refused = s.refused.wrapping_add(1);
    }
    s.acct.admit_input(ans.body.len() as u64);
    // With no `result_out` wired the chain ends at the effect: the answer's
    // status is the record's outcome and there is nothing to build.
    if s.out_chan < 0 {
        s.answers = s.answers.wrapping_add(1);
        if ans.status < 400 {
            s.acct.input_succeeded();
        } else {
            s.acct.input_failed();
        }
        return;
    }
    let mut fields = [Field {
        number: 0,
        value: Value::Null,
    }; MAX_PIPE_FIELDS];
    // A 2xx body is the answer and must decode. Any other status's body is the
    // provider's own — an error text, or nothing — and need not have the
    // answer's shape: when it does not decode, the record carries the status
    // and the carry alone, so the stages after still answer at once.
    let refusal = !(200..300).contains(&ans.status);
    let mut b = Builder::new();
    let decoded: Result<usize, bool> = if s.rdec_len > 0 {
        let prog = core::slice::from_raw_parts(s.rdec.as_ptr(), s.rdec_len as usize);
        let scr = core::slice::from_raw_parts_mut(s.dec_scratch.as_mut_ptr(), DEC_SCRATCH);
        match eval_decode_scratch(prog, ans.body, scr, &mut b, 100_000) {
            Err(e) => Err(e == EvalError::BuildOverflow),
            Ok(()) => {
                let m = b.message();
                if m.fields.len() > fields.len() {
                    Err(true)
                } else {
                    fields[..m.fields.len()].copy_from_slice(m.fields);
                    Ok(m.fields.len())
                }
            }
        }
    } else if ans.body.is_empty() {
        Ok(0)
    } else {
        decode_frame(ans.body, &mut fields).map_err(|e| e.over_bound())
    };
    let nf = match decoded {
        Ok(n) => n,
        Err(_) if refusal => 0,
        Err(over_bound) => {
            if over_bound {
                s.over_bound = s.over_bound.wrapping_add(1);
            }
            s.answers_failed = s.answers_failed.wrapping_add(1);
            s.acct.input_failed();
            return;
        }
    };
    // The carry and the status are the exchange's to say: any the body itself
    // carried under those numbers, or under a mapped field, is replaced.
    let map = s.map;
    let taken = |num: u32| {
        num == CARRY_FIELD
            || num == EXCHANGE_STATUS_FIELD
            || (map.status != 0 && num == map.status as u32)
            || (map.content_type != 0 && num == map.content_type as u32)
    };
    let mut n = 0usize;
    for i in 0..nf {
        let f = fields[i];
        if taken(f.number) {
            continue;
        }
        fields[n] = f;
        n += 1;
    }
    let mut ok = true;
    if !ans.carry.is_empty() {
        ok &= push_field(
            &mut fields,
            &mut n,
            Field {
                number: CARRY_FIELD,
                value: Value::Frame(ans.carry),
            },
        );
    }
    ok &= push_field(
        &mut fields,
        &mut n,
        Field {
            number: EXCHANGE_STATUS_FIELD,
            value: Value::Int(ans.status as i64),
        },
    );
    // An answer keeps its status and content type, not its header block: the
    // headers field is the request's alone.
    let answer_map = FieldMap { headers: 0, ..map };
    if ok {
        match response_fields(
            &answer_map,
            ans.status,
            ans.content_type,
            &[],
            &mut fields,
            n,
        ) {
            Ok(m) => n = m,
            Err(_) => ok = false,
        }
    }
    if !ok {
        s.over_bound = s.over_bound.wrapping_add(1);
        s.answers_failed = s.answers_failed.wrapping_add(1);
        s.acct.input_failed();
        return;
    }
    let out = core::slice::from_raw_parts_mut(s.reply_buf.as_mut_ptr(), REC_BUF);
    let rl = match encode_frame(
        &Message {
            fields: &fields[..n],
        },
        out,
    ) {
        Ok(rl) => rl,
        Err(_) => {
            s.answers_failed = s.answers_failed.wrapping_add(1);
            s.acct.input_failed();
            return;
        }
    };
    s.answers = s.answers.wrapping_add(1);
    let outch = SysChan::new(sys, s.out_chan);
    match s.reply_pending.stage(&outch, &s.reply_buf, rl) {
        Staged::Delivered => {
            s.acct.output_delivered_now(rl as u32);
            s.acct.input_succeeded();
        }
        Staged::Pending => s.acct.output_staged(rl as u32),
        Staged::Failed(_) => {
            s.reply_pending = Pending { off: 0, len: 0 };
            s.acct.output_failed_now();
            s.acct.input_failed();
        }
    }
}

/// Collect provider requests: take one request record from `request_in`. A
/// grant or refusal the collector owes is written on `response_out`. Returns
/// the length of the record frame built in `xframe` once a request is whole.
#[inline(never)]
unsafe fn provider_intake(s: &mut ModuleState, sys: &SyscallTable) -> Option<usize> {
    let n = read_record(s, sys, s.req_in_chan);
    if n == 0 {
        return None;
    }
    s.acct.admit_input(n as u64);
    let rec = core::slice::from_raw_parts(s.xrec.as_ptr(), n);
    let done = s.collector.accept(rec);
    if let Some((id, why)) = s.collector.take_refusal() {
        s.requests_refused = s.requests_refused.wrapping_add(1);
        s.acct.reject_input(0);
        respond_status(s, sys, &id, why.status());
        return None;
    }
    if let Some((id, bytes)) = s.collector.take_grant() {
        if let Some(m) = exchange::write_credit(&id, bytes, &mut s.resp_buf) {
            s.resp_box.send(sys, s.resp_out_chan, &s.resp_buf, m);
        }
    }
    let at = match done {
        Ok(Some(at)) => at,
        Ok(None) => return None,
        Err(_) => {
            s.requests_refused = s.requests_refused.wrapping_add(1);
            return None;
        }
    };
    s.in_slot = at as i8;
    let built = request_record(s, at);
    if built.is_none() {
        record_failed(s, sys);
    }
    built
}

/// Build the record a whole collected request becomes, into `xframe`: its body
/// through `decode` (or itself one record frame, or no fields when empty), and
/// its id, method, target and headers at the mapped fields.
#[inline(never)]
unsafe fn request_record(s: &mut ModuleState, at: usize) -> Option<usize> {
    // Read through a raw place: the request borrows the collector while the
    // fields beside it change.
    let req = (*core::ptr::addr_of!(s.collector)).request(at)?;
    s.in_id = req.id;
    let mut fields = [Field {
        number: 0,
        value: Value::Null,
    }; MAX_PIPE_FIELDS];
    let mut b = Builder::new();
    let nf = if s.dec_len > 0 {
        let dec = core::slice::from_raw_parts(s.dec.as_ptr(), s.dec_len as usize);
        let scr = core::slice::from_raw_parts_mut(s.dec_scratch.as_mut_ptr(), DEC_SCRATCH);
        if let Err(e) = eval_decode_scratch(dec, req.body, scr, &mut b, 100_000) {
            if e == EvalError::BuildOverflow {
                s.over_bound = s.over_bound.wrapping_add(1);
            }
            return None;
        }
        let m = b.message();
        if m.fields.len() > fields.len() {
            s.over_bound = s.over_bound.wrapping_add(1);
            return None;
        }
        fields[..m.fields.len()].copy_from_slice(m.fields);
        m.fields.len()
    } else if req.body.is_empty() {
        0
    } else {
        decode_frame(req.body, &mut fields).ok()?
    };
    let id = req.id.0;
    let map = s.map;
    let Ok(n) = request_fields(
        &map,
        &id,
        req.method,
        req.target,
        req.headers,
        &mut fields,
        nf,
    ) else {
        s.over_bound = s.over_bound.wrapping_add(1);
        return None;
    };
    let out = core::slice::from_raw_parts_mut(s.xframe.as_mut_ptr(), REC_BUF);
    encode_frame(
        &Message {
            fields: &fields[..n],
        },
        out,
    )
    .ok()
}

/// A relay's request record: a HEAD becomes a record of its mapped parts in
/// `xframe` (its flags, method and inline body kept for the HEAD it is
/// rewritten into); any other record is forwarded to `request_out` untouched.
#[inline(never)]
unsafe fn relay_intake(s: &mut ModuleState, sys: &SyscallTable) -> Option<usize> {
    let n = read_record(s, sys, s.req_in_chan);
    if n == 0 {
        return None;
    }
    let rec = core::slice::from_raw_parts(s.xrec.as_ptr(), n);
    if record_kind(rec) != Some(kind::HEAD) {
        s.relayed = s.relayed.wrapping_add(1);
        for (d, x) in s.req_buf.iter_mut().zip(rec.iter()) {
            *d = *x;
        }
        s.req_box.send(sys, s.req_out_chan, &s.req_buf, n);
        return None;
    }
    s.acct.admit_input(n as u64);
    let Some(exchange::Record::Head(h)) = exchange::parse_request(rec) else {
        // A HEAD that does not parse is still an exchange its requester
        // waits on.
        s.acct.input_failed();
        s.requests_refused = s.requests_refused.wrapping_add(1);
        if let Some(id) = exchange::record_id(rec) {
            respond_status(s, sys, &id, exchange::status::BAD_REQUEST);
        }
        return None;
    };
    s.relay_id = h.id;
    s.relay_open = true;
    s.relay_flags = h.flags;
    s.relay_method = h.method;
    s.relay_credit = h.resp_credit;
    // The inline body stays where it is in `xrec` until the HEAD is rewritten.
    s.relay_body_at = (n - h.body.len()) as u16;
    s.relay_body_len = h.body.len() as u16;
    let mut fields = [Field {
        number: 0,
        value: Value::Null,
    }; MAX_PIPE_FIELDS];
    let id = h.id.0;
    let built = request_fields(&s.map, &id, h.method, h.target, h.headers, &mut fields, 0)
        .ok()
        .and_then(|k| {
            let out = core::slice::from_raw_parts_mut(s.xframe.as_mut_ptr(), REC_BUF);
            encode_frame(
                &Message {
                    fields: &fields[..k],
                },
                out,
            )
            .ok()
        });
    if built.is_none() {
        s.over_bound = s.over_bound.wrapping_add(1);
        record_failed(s, sys);
    }
    built
}

/// Forward one response record from `response_in` to `response_out` untouched:
/// this node is on the answer path for another node's exchanges.
unsafe fn relay_response(s: &mut ModuleState, sys: &SyscallTable) {
    let n = read_record(s, sys, s.resp_in_chan);
    if n == 0 {
        return;
    }
    for (d, x) in s.resp_buf.iter_mut().zip(s.xrec.iter()).take(n) {
        *d = *x;
    }
    s.relayed = s.relayed.wrapping_add(1);
    s.resp_box.send(sys, s.resp_out_chan, &s.resp_buf, n);
}

/// Send the next request owed to the provider — lost to a LINK_DOWN or opened
/// while the link was down — once the link is up.
unsafe fn replay_step(s: &mut ModuleState, sys: &SyscallTable) {
    if !s.link_up {
        return;
    }
    let Some((at, req)) = s.inflight.next_replay() else {
        return;
    };
    let n = req.len();
    for (d, x) in s.req_buf.iter_mut().zip(req.iter()) {
        *d = *x;
    }
    s.inflight.replayed(at);
    s.replayed = s.replayed.wrapping_add(1);
    s.req_box.send(sys, s.req_out_chan, &s.req_buf, n);
}
// Evaluator + staged executor + container codec + hex codec — identical source
// to the host harness (tests/harness), all in ONE module so cross-references resolve.
mod pipe {
    use super::abi::SyscallTable;
    include!("../../common/vm_core.rs");
    include!("../../common/pipeline_core.rs");
    // The decision executor, so a decision can be a STAGE rather than a node.
    include!("../../common/decision_core.rs");
    include!("../../common/hex_core.rs");
    include!("../../common/ser_core.rs");
    include!("../../common/deser_core.rs");
    include!("../../common/version_core.rs");
    include!("../../common/lower_core.rs");
    include!("../../common/outcome_core.rs");
    include!("../../common/io_core.rs");
    include!("../../common/accounting_core.rs");
    include!("../../common/syschan_core.rs");
    include!("../../common/pipeline_reload_core.rs");
    // Exchange records ↔ records, beside the contract it maps.
    use super::exchange;
    include!("../../common/exchange_core.rs");
}

/// Validate every stage of every version with the scanner its kind names.
///
/// Returns true when any stage is bad. A compute stage is scanned for unknown
/// opcodes and truncation; a decision stage is scanned as a decision
/// container. Skipping either would admit a program this build cannot run.
fn scan_kinded_versions(vbin: &[u8], kinds: &[u8]) -> bool {
    let Some(vt) = parse_version_table(vbin) else {
        return true;
    };
    let mut v = 0usize;
    while let Some(e) = vt.entry(v) {
        let prog = e.prog;
        let ns = stage_count(prog);
        let mut i = 0usize;
        while i < ns {
            let Some(st) = stage_at(prog, i) else {
                return true;
            };
            let decision = kinds.get(i).is_some_and(|k| *k == STAGE_KIND_DECISION);
            let map = kinds.get(i).is_some_and(|k| *k == STAGE_KIND_MAP);
            let bad = if decision {
                scan_decision_container(st.code).is_err()
            } else if map {
                scan_map_container(st.code).is_err()
            } else {
                scan_code(st.code).is_err()
            };
            if bad {
                return true;
            }
            i += 1;
        }
        v += 1;
    }
    false
}

/// Runs each stage with the executor its kind names.
///
/// A DECISION is one input, one output and the same decode/evaluate/encode
/// shape as a compute stage; only the program format differs. Threading it
/// here makes a chain that routes mid-way ONE node instead of
/// `pipeline -> decision -> pipeline`, so the two channel hops either side of
/// the decision are not paid.
struct KindedEval;

impl StageEval for KindedEval {
    fn eval(
        &self,
        stage: &Stage,
        src: &[u8],
        dst: &mut [u8],
        spent: &mut u64,
    ) -> Result<usize, PipeError> {
        if stage.kind == STAGE_KIND_MAP {
            return run_map_stage(stage.code, src, dst, spent);
        }
        if stage.kind != STAGE_KIND_DECISION {
            return run_stage_metered(stage, src, dst, spent);
        }
        run_decision_stage(stage, src, dst, spent)
    }
}

/// One DECISION stage: decode, route, encode the fired branch's record.
#[inline(never)]
fn run_decision_stage(
    stage: &Stage,
    src: &[u8],
    dst: &mut [u8],
    spent: &mut u64,
) -> Result<usize, PipeError> {
    let mut fields = [Field {
        number: 0,
        value: Value::Null,
    }; MAX_PIPE_FIELDS];
    let nf = decode_frame(src, &mut fields)?;
    let params = [Message {
        fields: &fields[..nf],
    }];
    let mut builder = Builder::new();
    let mut sbuf = [0u8; STAGE_SCRATCH_CAP];
    let mut scratch = Scratch::new(&mut sbuf);
    let mut w = 0u64;
    let fired = run_decision_metered(stage.code, &params, &mut builder, &mut scratch, &mut w);
    *spent += w;
    match fired {
        // Which branch fired is an audit fact the standalone `decision`
        // module reports on its own instrument. Inline, the outcome record
        // IS the report: the next stage sees what a downstream node would.
        Ok(_) => encode_frame_scratch(&builder.message(), &scratch, dst),
        Err(_) => Err(PipeError::NotConstructed),
    }
}

use pipe::{
    admit_frame, decode_frame, drain_all, encode_frame, encode_frame_scratch, eval_bytes,
    eval_decode_scratch, frame_len, hex_decode, is_data_field, lower_stages_kinded,
    parse_version_table, pipeline_reload_kinded, record_id, request_fields, request_head,
    response_fields, response_head, run_decision_metered, run_map_stage, run_stage_metered,
    run_stages_metered, run_stages_with, scan_code, scan_decision_container, scan_map_container,
    scan_version_table, stage_at, stage_count, version_selector_from_frame, Accounting, Admit,
    Answer, Builder, EvalError, Field, FieldMap, Inflight, Message, Mode, Pending, PipeError,
    Scratch, Stage, StageEval, Staged, SysChan, Value, ACCT_IS_GAUGE, ACCT_METRIC_COUNT,
    CARRY_FIELD, EXCHANGE_STATUS_FIELD, MAX_NODE_STAGES, MAX_PIPE_FIELDS, STAGE_KIND_COMPUTE,
    STAGE_KIND_DECISION, STAGE_KIND_MAP, STAGE_SCRATCH_CAP,
};

// Telemetry emit helpers — crate root, after the SDK runtime so its primitives are in scope.
include!("../../common/telemetry_core.rs");

const HEX_BUF: usize = 4096;
const PROG_BUF: usize = 2048;
/// The trailing encoder's output and the ingress decoder's frame: one record,
/// tiered with it. An encoded body becomes an exchange record's body or is
/// copied into `out_buf` for delivery, and a decoded frame is a stage input.
const ENC_BUF: usize = REC_BUF;

/// One record, in bytes: the read buffer, both stage ping-pong buffers and
/// the write buffer.
///
/// A `pipeline` carries whatever record its graph carries, and the failure
/// mode when one overruns is why the bound is generous: the record is not
/// truncated, it is DROPPED. `channel_read` stops at the buffer, the codec
/// then reads a length the rest of the record was going to satisfy, fails,
/// and the record vanishes with a counter. From the client that is a request
/// that never answers, which is the hardest possible shape to diagnose from
/// the outside.
///
/// A single compact JWS is ~350 bytes, and an HTTP envelope carrying one
/// carries the path and header block beside it.
///
/// Which is why the bound is DERIVED from the state arena rather than fixed: the
/// records that justify a generous ceiling are the ones a constrained target
/// cannot receive. Three 4 KiB record buffers plus two 8 KiB version tables would
/// be half of a 64 KiB arena, and a JWS inside an HTTP envelope cannot reach a
/// graph with no room for an HTTP stack beside this module. Five hundred and
/// twelve bytes is a sensor record and a decision outcome, which is what such a
/// graph carries.
///
/// A CAPACITY choice rather than a variant, because the drop-not-truncate failure
/// mode is identical at either size: an overrun is refused and counted the same
/// way, so the two builds differ in what they can hold and in nothing else. The
/// manifest declares the matching `max_record` per target, so the advertised
/// ceiling and the buffer behind it cannot disagree.
///
/// `expression` and `decision` carry the same bound, and must: the three sit on
/// one channel family, so a node that admitted more than its downstream would
/// pass records that node must then refuse. `limit_register.md` records the
/// coupling and the gate holds the three to one derivation.
const TINY: bool = abi::config::kernel::STATE_ARENA_SIZE <= 64 * 1024;
const REC_BUF: usize = if TINY { 512 } else { 4096 };
/// Backing buffer for the version table (holds every loaded version's program;
/// mutable for hot reload). Larger than one program so several versions coexist.
///
/// Held in TWO copies (active + candidate) so a reload can be staged and
/// rejected without disturbing what is running, so this constant costs twice
/// what it says. On a 64 KiB arena that is a quarter of everything for a
/// blue/green capability a sensor node does not exercise — such a node is
/// reconfigured by OTA, not by hot-swapping program versions under load — so it
/// carries one program's worth plus headroom instead.
const VBIN_BUF: usize = if TINY { 3072 } else { 8192 };
/// Control-message scratch for the `ctrl_input` port (hot-reload ops).
const CTRL_BUF: usize = 4096;

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_chan: i32,
    out_chan: i32,
    in_buf: [u8; REC_BUF],
    buf_a: [u8; REC_BUF],
    buf_b: [u8; REC_BUF],
    out_buf: [u8; REC_BUF],
    // `ir_stages` hex, lowered into `prog`, which becomes the single default
    // version when no `versions` table is given.
    hex: [u8; HEX_BUF],
    hex_len: u16,
    prog: [u8; PROG_BUF],
    ir_scratch: [u8; PROG_BUF],
    ver_hex: [u8; HEX_BUF],
    ver_hex_len: u16,
    vbin: [u8; VBIN_BUF],
    vbin_len: u16,
    /// The candidate table a reload is staged and validated in before it
    /// replaces `vbin`.
    vbin_cand: [u8; VBIN_BUF],
    /// Declared stage kinds, parallel to the stage container. Empty means
    /// every stage is compute.
    stage_kinds: [u8; MAX_NODE_STAGES],
    stage_kinds_len: u8,
    /// One retained output frame on `result_out`, drained before any new input
    /// is admitted.
    pending: Pending,

    // ── Exchange ports: in[1] `response_in`, out[1] `request_out`,
    //    in[2] `request_in`, out[2] `response_out`; `-1` where unwired.
    resp_in_chan: i32,
    req_out_chan: i32,
    req_in_chan: i32,
    resp_out_chan: i32,
    /// Which data fields carry an exchange's id, method, target, headers,
    /// status and content type.
    map: FieldMap,
    /// The method a request asks when its record names none.
    method: u8,
    /// The content type an answer carries when its record names none.
    content_type: [u8; CT_BUF],
    content_type_len: u8,
    /// This node's result is the answer to the exchange its record names.
    answer: bool,
    /// This node answers each collected request with its status once the
    /// record resolves, as a sink.
    ack: bool,
    /// Requests collected as a provider, and the one whose record is in flight
    /// (`in_slot` `-1` when none).
    collector: Collector<XSLOTS, KEY_BUF, XHDRS, XBODY>,
    in_slot: i8,
    in_id: ExchangeId,
    /// The collected request's own record answered it (`answer`, or a
    /// status its stages named), so `ack` owes nothing more.
    in_answered: bool,
    /// The requester's exchanges in flight.
    inflight: Inflight<WINDOW, KEY_BUF, XREC, XBODY>,
    /// The provider's backend link is up: replays may go.
    link_up: bool,
    /// One exchange record as read.
    xrec: [u8; XREC],
    /// The record a collected or relayed request becomes.
    xframe: [u8; REC_BUF],
    /// Request records written on `request_out`, held when the ring is full.
    req_buf: [u8; XREC],
    req_box: ExchangeOutbox,
    /// Response records written on `response_out`, held likewise.
    resp_buf: [u8; XREC],
    resp_box: ExchangeOutbox,
    /// The relayed HEAD in flight: its exchange, whether it is still owed an
    /// outcome, its flags, method, response credit, and where its inline body
    /// sits in `xrec`.
    relay_id: ExchangeId,
    relay_open: bool,
    relay_flags: u8,
    relay_method: u8,
    relay_credit: u32,
    relay_body_at: u16,
    relay_body_len: u16,
    /// The program that turns an answer body into a record. Without it an
    /// answer body must be one record frame.
    rdec_hex: [u8; RDEC_HEX_BUF],
    rdec_hex_len: u16,
    rdec: [u8; RDEC_PROG_BUF],
    rdec_len: u16,
    /// One retained answer record, drained before another answer is taken.
    reply_pending: Pending,
    reply_buf: [u8; REC_BUF],
    /// Requester: answers with a status of 400 or above, answers turned into
    /// records, answers that could not be, requests sent again after a
    /// LINK_DOWN, and LINK_DOWN signals seen.
    refused: u32,
    answers: u32,
    answers_failed: u32,
    replayed: u32,
    link_downs: u32,
    /// Provider: requests answered as a sink, and requests refused.
    answered: u32,
    requests_refused: u32,
    /// Records forwarded untouched as a relay.
    relayed: u32,
    /// Records refused because their carry exceeds `KEY_BUF` — refused, never
    /// truncated, since a clipped context rejoins the wrong request.
    carry_refused: u32,
    /// Records refused for exceeding a bound: more fields than the field table
    /// holds, or more elements than a map stage declares. A subset of
    /// `inputs_failed`, counted apart because the remedy is capacity, not a fix.
    over_bound: u32,

    // Control port: hot-reload ops (add version / flip default / remove).
    ctrl_chan: i32,
    ctrl_buf: [u8; CTRL_BUF],
    reloads: u32,
    rejected: u32,    // hot-reload control messages the table rejected
    unavailable: u32, // records fail-closed because their version was not loaded
    /// 1 = configuration fault at init/reload: the node refuses input
    /// (declared metric; the named reason was logged once at error level).
    faulted: u32,

    // Optional trailing encoder: a byte-serialization program applied to the
    // final record to produce a body (an exchange's) or a wire payload. When
    // present, the module emits those bytes instead of a frame.
    enc_hex: [u8; HEX_BUF],
    enc_hex_len: u16,
    enc: [u8; PROG_BUF],
    enc_len: u16,
    enc_out: [u8; ENC_BUF],

    // Optional front-end decoder: a byte-deserialization program that parses a
    // raw protocol chunk, or a collected request's body, into a record frame
    // before the stages.
    dec_hex: [u8; HEX_BUF],
    dec_hex_len: u16,
    dec: [u8; PROG_BUF],
    dec_len: u16,
    dec_out: [u8; ENC_BUF],
    /// Where a decode program builds values (`CAT`, `HEX` — a key assembled
    /// from parts, an identity as hex); reused per record.
    dec_scratch: [u8; DEC_SCRATCH],

    /// The common accounting taxonomy: a delivered output (a transformed record OR the fail-closed
    /// VERSION_UNAVAILABLE marker) resolves its input as `inputs_succeeded`; a
    /// terminal processing failure before any deliverable output is `inputs_failed`.
    /// `reloads`/`rejected` count CONTROL messages and stay outside this taxonomy.
    acct: Accounting,
    /// A string param overflowed its buffer during parsing (truncated) — a fault,
    /// since a truncated stage/version table could decode to a different program.
    param_overflow: bool,
    /// Current operating mode (`Mode` as u8), published as `module_mode`.
    mode: u8,
    /// Wall-clock ms of the last telemetry publish (throttle state).
    tlm_last_ms: u64,
}

define_params! {
    ModuleState;

    // `str_chunked`: the encode program arrives as TLV entries of at most
    // 255 bytes under the same tag and this handler APPENDS each one.
    2, encode, str_chunked, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.enc_hex_len as usize) < HEX_BUF {
            s.enc_hex[s.enc_hex_len as usize] = *d.add(i);
            s.enc_hex_len += 1;
            i += 1;
        }
        if i < len { s.param_overflow = true; }
    };

    // `str_chunked` like `encode`: a decode program that reads a whole request
    // (a target, a header, a JSON body, a key) is past one 255-byte entry.
    3, decode, str_chunked, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.dec_hex_len as usize) < HEX_BUF {
            s.dec_hex[s.dec_hex_len as usize] = *d.add(i);
            s.dec_hex_len += 1;
            i += 1;
        }
        if i < len { s.param_overflow = true; }
    };

    4, versions, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.ver_hex_len as usize) < HEX_BUF {
            s.ver_hex[s.ver_hex_len as usize] = *d.add(i);
            s.ver_hex_len += 1;
            i += 1;
        }
        if i < len { s.param_overflow = true; }
    };

    // The program that turns an answer's body into a record (see
    // `response_in`). Without it an answer body must be one record frame.
    7, reply_decode, str_chunked, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.rdec_hex_len as usize) < RDEC_HEX_BUF {
            s.rdec_hex[s.rdec_hex_len as usize] = *d.add(i);
            s.rdec_hex_len += 1;
            i += 1;
        }
        if i < len { s.param_overflow = true; }
    };

    // Which executor runs each stage, one hex byte per stage, parallel to
    // `ir_stages`: `00` compute, `01` decision, `02` map. Absent means all
    // compute, which is what a graph that names no executors is asking for.
    // One vector serves every version in the table, hot-reloaded ones included.
    //
    // Declared here rather than inside the stage container because that
    // container's bytes are recorded in a frozen corpus and in every emitted
    // graph: changing its shape would invalidate answers that cannot be
    // regenerated.
    6, stage_kinds, str, 0 => |s, d, len| {
        let mut raw = [0u8; MAX_NODE_STAGES * 2];
        // More kinds than stages this build runs: refused, not cut short.
        if len > raw.len() { s.param_overflow = true; }
        let n = if len > raw.len() { raw.len() } else { len };
        let mut i = 0usize;
        while i < n {
            raw[i] = *d.add(i);
            i += 1;
        }
        let mut out = [0u8; MAX_NODE_STAGES];
        if let Some(nb) = hex_decode(&raw[..n], &mut out) {
            s.stage_kinds = [STAGE_KIND_COMPUTE; MAX_NODE_STAGES];
            let mut k = 0usize;
            while k < nb && k < MAX_NODE_STAGES {
                s.stage_kinds[k] = out[k];
                k += 1;
            }
            s.stage_kinds_len = nb as u8;
        } else {
            s.param_overflow = true;
        }
    };
    // A shipped IR-stages container (hex), lowered to a bytecode-stages container
    // at load (each stage's cost re-derived). `str_chunked`: the stage list
    // arrives as TLV entries of at most 255 bytes under the same tag and this
    // handler APPENDS each one.
    5, ir_stages, str_chunked, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.hex_len as usize) < HEX_BUF {
            s.hex[s.hex_len as usize] = *d.add(i);
            s.hex_len += 1;
            i += 1;
        }
        if i < len { s.param_overflow = true; }
    };

    // Which data fields carry the parts of an exchange that are not its body
    // (`exchange_core::FieldMap`); `0` leaves a part unmapped. Each must name a
    // data field, `1..=239`.
    8, id_field, u8, 0 => |s, d, len| { s.map.id = p_u8(d, len, 0, 0); };
    9, method_field, u8, 0 => |s, d, len| { s.map.method = p_u8(d, len, 0, 0); };
    10, target_field, u8, 0 => |s, d, len| { s.map.target = p_u8(d, len, 0, 0); };
    11, headers_field, u8, 0 => |s, d, len| { s.map.headers = p_u8(d, len, 0, 0); };
    12, status_field, u8, 0 => |s, d, len| { s.map.status = p_u8(d, len, 0, 0); };
    13, content_type_field, u8, 0 => |s, d, len| { s.map.content_type = p_u8(d, len, 0, 0); };
    // The method a request asks when its record names none: the exchange
    // contract's codes (`METHOD_PUBLISH` for a durable destination).
    14, method, u8, 0 => |s, d, len| { s.method = p_u8(d, len, 0, 0); };
    // The content type an answer carries when its record names none.
    15, content_type, str, 0 => |s, d, len| {
        if len > CT_BUF { s.param_overflow = true; }
        let n = if len > CT_BUF { CT_BUF } else { len };
        let mut i = 0usize;
        while i < n {
            s.content_type[i] = *d.add(i);
            i += 1;
        }
        s.content_type_len = n as u8;
    };
    // This node's result is the response to the exchange its record names.
    16, answer, u8, 0 => |s, d, len| { s.answer = p_u8(d, len, 0, 0) != 0; };
    // Answer each collected request with its status once its record resolves.
    17, ack, u8, 0 => |s, d, len| { s.ack = p_u8(d, len, 0, 0) != 0; };
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<ModuleState>() as u32
}

// The same figure as data, so `pack` records this engine's resident footprint
// in its manifest and a graph's state-arena demand is summable at compose time
// rather than discovered when the device fails to load it.
declare_module_state_bytes!(ModuleState);

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
#[allow(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "the fluxor module ABI entry point: the runtime owns these pointers and \
              their validity is the ABI's contract, and the signature is fixed by that \
              contract rather than chosen here"
)]
pub extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    ctrl_chan: i32,
    params: *const u8,
    params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    unsafe {
        if syscalls.is_null() || state.is_null() {
            return -1;
        }
        if state_size < core::mem::size_of::<ModuleState>() {
            return -2;
        }
        let s = &mut *(state as *mut ModuleState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.in_chan = in_chan;
        s.out_chan = out_chan;
        s.ctrl_chan = ctrl_chan;
        s.in_buf = [0u8; REC_BUF];
        s.buf_a = [0u8; REC_BUF];
        s.buf_b = [0u8; REC_BUF];
        s.out_buf = [0u8; REC_BUF];
        s.hex_len = 0;
        s.enc_hex_len = 0;
        s.enc_len = 0;
        s.dec_hex_len = 0;
        s.dec_len = 0;
        s.ver_hex_len = 0;
        s.vbin_len = 0;
        s.vbin_cand = [0u8; VBIN_BUF];
        s.pending = Pending { off: 0, len: 0 };
        s.stage_kinds = [STAGE_KIND_COMPUTE; MAX_NODE_STAGES];
        s.stage_kinds_len = 0;
        s.resp_in_chan = dev_channel_port(sys, 0, 1);
        s.req_out_chan = dev_channel_port(sys, 1, 1);
        s.req_in_chan = dev_channel_port(sys, 0, 2);
        s.resp_out_chan = dev_channel_port(sys, 1, 2);
        s.map = FieldMap::default();
        s.method = 0;
        s.content_type = [0u8; CT_BUF];
        s.content_type_len = 0;
        s.answer = false;
        s.ack = false;
        s.collector = Collector::new();
        s.in_slot = -1;
        s.in_id = ExchangeId::NONE;
        s.in_answered = false;
        s.inflight = Inflight::new();
        s.link_up = true;
        s.req_box = ExchangeOutbox::new();
        s.resp_box = ExchangeOutbox::new();
        s.relay_id = ExchangeId::NONE;
        s.relay_open = false;
        s.relay_flags = 0;
        s.relay_method = 0;
        s.relay_credit = 0;
        s.relay_body_at = 0;
        s.relay_body_len = 0;
        s.rdec_hex_len = 0;
        s.rdec_len = 0;
        s.reply_pending = Pending { off: 0, len: 0 };
        s.refused = 0;
        s.answers = 0;
        s.answers_failed = 0;
        s.replayed = 0;
        s.link_downs = 0;
        s.answered = 0;
        s.requests_refused = 0;
        s.relayed = 0;
        s.carry_refused = 0;
        s.over_bound = 0;
        s.param_overflow = false;
        s.mode = Mode::AwaitingConfig.as_u8();
        // Backdate so the FIRST telemetry publish fires promptly (dev_millis is
        // uptime, near 0 at startup), not only after one full interval.
        s.tlm_last_ms = dev_millis(sys).wrapping_sub(TLM_INTERVAL_MS);
        s.reloads = 0;
        s.rejected = 0;
        s.unavailable = 0;
        s.faulted = 0;

        parse_tlv(s, params, params_len);

        // Build the version table. A `versions` param is a ready-made table; an
        // `ir_stages` param becomes a single default-version table.
        // Lower the shipped `ir_stages` IR container into the bytecode-stages
        // container `s.prog` (each stage's cost re-derived at load).
        // FAULT DISCIPLINE: a param that was PROVIDED but is broken — bad
        // hex, a container that fails to lower, a program this build cannot
        // run — is a configuration fault, named once at error level; the
        // node then refuses input (visible backpressure) rather than sitting
        // inert while records vanish. "No param" stays a distinct message.
        let mut fault: &'static [u8] = b"";
        let prog_n = if s.hex_len == 0 {
            None
        } else {
            match hex_decode(&s.hex[..s.hex_len as usize], &mut s.ir_scratch) {
                None => {
                    fault = b"[pipeline] FAULT: ir_stages param is not valid hex";
                    None
                }
                Some(flen) => match lower_stages_kinded(
                    &s.ir_scratch[..flen],
                    &s.stage_kinds[..s.stage_kinds_len as usize],
                    &mut s.prog,
                ) {
                    Err(_) => {
                        fault = b"[pipeline] FAULT: ir_stages container failed to lower";
                        None
                    }
                    Ok(n) => Some(n),
                },
            }
        };

        if s.ver_hex_len > 0 {
            match hex_decode(&s.ver_hex[..s.ver_hex_len as usize], &mut s.vbin) {
                Some(n) => s.vbin_len = n as u16,
                None => fault = b"[pipeline] FAULT: versions param is not valid hex",
            }
        } else if let Some(n) = prog_n {
            // [1][0][digest:8 = 0][tag_len:0][prog_len:u16][prog]
            let mut w = 0usize;
            s.vbin[0] = 1;
            s.vbin[1] = 0;
            w += 2;
            let mut k = 0;
            while k < 8 {
                s.vbin[w] = 0;
                w += 1;
                k += 1;
            }
            s.vbin[w] = 0; // tag_len (empty tag = default only)
            w += 1;
            let pl = (n as u16).to_le_bytes();
            s.vbin[w] = pl[0];
            s.vbin[w + 1] = pl[1];
            w += 2;
            let mut j = 0;
            while j < n && w < VBIN_BUF {
                s.vbin[w] = s.prog[j];
                w += 1;
                j += 1;
            }
            s.vbin_len = w as u16;
        }

        match hex_decode(&s.enc_hex[..s.enc_hex_len as usize], &mut s.enc) {
            Some(n) => s.enc_len = n as u16,
            None => {
                s.enc_len = 0;
                if s.enc_hex_len > 0 {
                    fault = b"[pipeline] FAULT: encode param is not valid hex";
                }
            }
        }
        match hex_decode(&s.rdec_hex[..s.rdec_hex_len as usize], &mut s.rdec) {
            Some(n) => s.rdec_len = n as u16,
            None => {
                s.rdec_len = 0;
                if s.rdec_hex_len > 0 {
                    fault = b"[pipeline] FAULT: reply_decode param is not valid hex";
                }
            }
        }
        match hex_decode(&s.dec_hex[..s.dec_hex_len as usize], &mut s.dec) {
            Some(n) => s.dec_len = n as u16,
            None => {
                s.dec_len = 0;
                if s.dec_hex_len > 0 {
                    fault = b"[pipeline] FAULT: decode param is not valid hex";
                }
            }
        }
        // Load-time scan of every version's stage programs: unknown opcodes,
        // truncation, builtins not in this build's variant. Refused here,
        // once — never per-record. (`encode`/`decode` are ser/rd byte-VM
        // programs with their own opcode space; they carry no CALL and are
        // validated by their own evaluators' fail-closed paths.)
        // A DECISION stage's body is a decision container, not VM code, so the
        // opcode scan would reject it for a fault it does not have. Each kind
        // is validated by its own scanner; neither is skipped.
        let kinded = s.stage_kinds_len > 0;
        if s.vbin_len > 0 {
            let bad = if kinded {
                scan_kinded_versions(
                    &s.vbin[..s.vbin_len as usize],
                    &s.stage_kinds[..s.stage_kinds_len as usize],
                )
            } else {
                scan_version_table(&s.vbin[..s.vbin_len as usize]).is_err()
            };
            if bad {
                fault = b"[pipeline] FAULT: a stage needs an opcode/builtin not in this build";
            }
        }
        if s.param_overflow {
            fault = b"[pipeline] FAULT: a param exceeded its buffer and was truncated";
        }
        // The exchange wiring must be one the roles define: a provider owes
        // grants and refusals on `response_out`, a requester its answers come
        // back on `response_in`, and every mapped field is a data field.
        let m = s.map;
        let mapped_ok = [
            m.id,
            m.method,
            m.target,
            m.headers,
            m.status,
            m.content_type,
        ]
        .iter()
        .all(|n| *n == 0 || is_data_field(*n));
        if !mapped_ok {
            fault = b"[pipeline] FAULT: an exchange field map names a reserved field";
        }
        let relay_req = s.req_in_chan >= 0 && s.req_out_chan >= 0;
        if s.req_in_chan >= 0 && s.resp_out_chan < 0 {
            fault = b"[pipeline] FAULT: request_in is wired without response_out";
        }
        if s.req_out_chan >= 0 && s.resp_in_chan < 0 {
            fault = b"[pipeline] FAULT: request_out is wired without response_in";
        }
        if s.answer && (s.resp_out_chan < 0 || (m.id == 0 && s.req_in_chan < 0)) {
            fault = b"[pipeline] FAULT: answer needs response_out and an id field";
        }
        if s.ack && s.answer {
            fault = b"[pipeline] FAULT: ack and answer are exclusive";
        }
        if relay_req && m.id == 0 {
            fault = b"[pipeline] FAULT: a relay needs an id field";
        }
        s.acct = Accounting::default();
        if !fault.is_empty() {
            s.vbin_len = 0;
            s.faulted = 1;
            dev_log(sys, 1, fault.as_ptr(), fault.len());
        } else if s.vbin_len == 0
            && s.enc_len == 0
            && s.dec_len == 0
            && s.req_in_chan < 0
            && s.req_out_chan < 0
            && !s.answer
        {
            dev_log(sys, 3, b"[pipeline] no program param".as_ptr(), 27);
        } else {
            dev_log(sys, 3, b"[pipeline] init".as_ptr(), 15);
        }
        0
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;
        // A relay is on its exchanges' answer path too, so a request it cannot
        // pass on is still answered; the provider's answers pass through it.
        let relay_req = s.req_in_chan >= 0 && s.req_out_chan >= 0;
        let requester = s.req_out_chan >= 0 && s.resp_in_chan >= 0 && !relay_req;
        let relay_resp = s.resp_in_chan >= 0 && s.resp_out_chan >= 0 && !requester;
        // A node with no program is still configured when it plays an exchange
        // role: a requester or a relay with no stages carries records as they
        // are, and so does a provider.
        let unconfigured = s.vbin_len == 0
            && s.enc_len == 0
            && s.dec_len == 0
            && s.req_in_chan < 0
            && s.req_out_chan < 0
            && !s.answer;

        s.mode = if s.faulted != 0 {
            Mode::Faulted.as_u8()
        } else if unconfigured {
            Mode::AwaitingConfig.as_u8()
        } else if !s.pending.is_empty()
            || s.req_box.holding()
            || s.resp_box.holding()
            || (requester && !s.inflight.has_room())
        {
            // A full in-flight window or a held exchange record blocks exactly
            // as a retained output does: backpressure by channel, never by
            // dropping.
            Mode::OutputBlocked.as_u8()
        } else {
            Mode::Ready.as_u8()
        };
        if let Some((midx, t)) = tlm_tick(sys, &mut s.tlm_last_ms) {
            // ids 0..13: the baseline accounting block.
            acct_emit(sys, midx, t, 0, &s.acct);
            // ids 14..: pipeline's own instruments. module_mode, the init/reload
            // fault flag, the two reload-CONTROL counters (control-plane counters,
            // not record dispositions), and version-unavailable (a refinement of
            // which succeeded inputs were routed to the fail-closed marker rather
            // than transformed).
            let b = ACCT_METRIC_COUNT as u16;
            tlm_gauge(sys, midx, t, b, s.mode as u64);
            tlm_gauge(sys, midx, t, b + 1, s.faulted as u64);
            tlm_counter(sys, midx, t, b + 2, s.reloads as u64);
            tlm_counter(sys, midx, t, b + 3, s.rejected as u64);
            tlm_counter(sys, midx, t, b + 4, s.unavailable as u64);
            // work units — VM instructions across every stage executed (incl. routes).
            tlm_counter(sys, midx, t, b + 5, s.acct.work_units);
            // Requester: the in-flight window, and what became of what left it.
            tlm_gauge(sys, midx, t, b + 6, s.inflight.open_count() as u64);
            tlm_counter(sys, midx, t, b + 7, s.refused as u64);
            tlm_counter(sys, midx, t, b + 8, s.answers as u64);
            tlm_counter(sys, midx, t, b + 9, s.answers_failed as u64);
            tlm_counter(sys, midx, t, b + 10, s.replayed as u64);
            tlm_counter(sys, midx, t, b + 11, s.link_downs as u64);
            // Provider: requests answered as a sink, and requests refused.
            tlm_counter(sys, midx, t, b + 12, s.answered as u64);
            tlm_counter(sys, midx, t, b + 13, s.requests_refused as u64);
            tlm_counter(sys, midx, t, b + 14, s.relayed as u64);
            tlm_counter(sys, midx, t, b + 15, s.carry_refused as u64);
            tlm_counter(sys, midx, t, b + 16, s.over_bound as u64);
        }

        // 0. A held exchange record goes before anything else: every answer,
        //    grant and request is delivered, never dropped, and in order.
        if !s.req_box.flush(sys, s.req_out_chan, &s.req_buf)
            || !s.resp_box.flush(sys, s.resp_out_chan, &s.resp_buf)
        {
            return 0;
        }

        // 0b. The requester's answers, and the replays a LINK_DOWN owes.
        if requester {
            requester_step(s, sys);
            replay_step(s, sys);
            if s.req_box.holding() {
                return 0;
            }
        }
        // 0c. Another node's answers passing through to the requester.
        if relay_resp {
            relay_response(s, sys);
            if s.resp_box.holding() {
                return 0;
            }
        }

        // 1. Hot reload — TRANSACTIONAL. Apply the control op to the candidate
        //    table, validate it, and copy it over the active table ONLY on success. A
        //    rejected candidate leaves the active generation byte-identical, so it
        //    cannot fault a healthy node.
        if s.ctrl_chan >= 0 {
            let cp = (sys.channel_poll)(s.ctrl_chan, 0x01);
            if cp > 0 && (cp as u32 & 0x01) != 0 {
                let cn = (sys.channel_read)(s.ctrl_chan, s.ctrl_buf.as_mut_ptr(), s.ctrl_buf.len());
                if cn > 0 {
                    let vbin_len = s.vbin_len as usize;
                    let active = core::slice::from_raw_parts(s.vbin.as_ptr(), vbin_len);
                    let msg = core::slice::from_raw_parts(s.ctrl_buf.as_ptr(), cn as usize);
                    // The candidate is lowered and validated with the stage
                    // kinds the running table was loaded with, exactly as at
                    // init: a decision or map body is neither lowered as
                    // expression IR nor scanned as VM code.
                    let kinds = &s.stage_kinds[..s.stage_kinds_len as usize];
                    let reloaded = pipeline_reload_kinded(
                        active,
                        &mut s.vbin_cand,
                        VBIN_BUF,
                        msg,
                        kinds,
                        |t: &[u8]| {
                            if kinds.is_empty() {
                                scan_version_table(t).is_ok()
                            } else {
                                !scan_kinded_versions(t, kinds)
                            }
                        },
                    );
                    match reloaded {
                        Ok(nu) => {
                            // Element by element: a slice copy whose lengths
                            // the compiler cannot prove equal carries a panic
                            // path, and a module image links none.
                            for (d, x) in s.vbin.iter_mut().zip(s.vbin_cand.iter()).take(nu) {
                                *d = *x;
                            }
                            s.vbin_len = nu as u16;
                            s.reloads = s.reloads.wrapping_add(1);
                            dev_log(sys, 3, b"[pipeline] reload".as_ptr(), 17);
                        }
                        // Active table untouched — a rejected reload cannot damage it.
                        Err(_) => s.rejected = s.rejected.wrapping_add(1),
                    }
                }
            }
        }

        if (s.in_chan < 0 && s.req_in_chan < 0) || s.faulted != 0 || unconfigured {
            return 0;
        }
        let outch = SysChan::new(sys, s.out_chan);

        // 2. Deliver any retained output before admitting new input. One record
        //    is in flight at a time, so a blocked write can never lose a mid-batch
        //    frame.
        if !s.pending.is_empty() {
            let plen = s.pending.len as u32;
            match s.pending.drain(&outch, &s.out_buf) {
                // The retained frame is this in-flight record's one defined output;
                // its delivery resolves the record.
                Staged::Delivered => {
                    s.acct.output_drained(plen);
                    record_succeeded(s, sys);
                }
                Staged::Pending => return 0,
                Staged::Failed(_) => {
                    s.pending = Pending { off: 0, len: 0 };
                    s.acct.output_failed_pending(plen);
                    record_failed(s, sys);
                }
            }
            return 0;
        }

        // A full in-flight window is backpressure: nothing is admitted until the
        // provider answers, so this module never runs ahead of it.
        if requester && !s.inflight.has_room() {
            return 0;
        }

        // 3. Admit ONE input unit: a whole collected request (provider), a
        //    request HEAD to rewrite (relay), or a record on `record_in` —
        //    mode (a) no decoder: a whole typed frame, peeked whole; mode (b)
        //    decoder: a raw protocol chunk decoded to one frame (no partial-read
        //    state — a message split across reads fails the decode, counted).
        let frame_ptr: *const u8;
        let frame_bytes: usize;
        if relay_req {
            match relay_intake(s, sys) {
                Some(n) => {
                    frame_ptr = s.xframe.as_ptr();
                    frame_bytes = n;
                }
                None => return 0,
            }
        } else if s.req_in_chan >= 0 {
            match provider_intake(s, sys) {
                Some(n) => {
                    frame_ptr = s.xframe.as_ptr();
                    frame_bytes = n;
                }
                None => return 0,
            }
        } else if s.dec_len > 0 {
            if !readable(sys, s.in_chan) {
                return 0;
            }
            let n = (sys.channel_read)(s.in_chan, s.in_buf.as_mut_ptr(), REC_BUF);
            if n <= 0 {
                return 0;
            }
            let raw_len = n as usize;
            // A raw protocol chunk accepted for processing is one admitted record.
            s.acct.admit_input(raw_len as u64);
            let inp = core::slice::from_raw_parts(s.in_buf.as_ptr(), raw_len);
            match decode_into(s, inp) {
                Some(rl) => {
                    frame_ptr = s.dec_out.as_ptr();
                    frame_bytes = rl;
                }
                None => {
                    record_failed(s, sys);
                    return 0;
                }
            }
        } else {
            let inch = SysChan::new(sys, s.in_chan);
            match admit_frame(&inch, &mut s.in_buf, frame_len) {
                Admit::Complete(nn) => {
                    s.acct.admit_input(nn as u64);
                    frame_ptr = s.in_buf.as_ptr();
                    frame_bytes = nn;
                }
                Admit::Empty | Admit::NeedMore => return 0,
                Admit::BoundaryLost => {
                    // A frame beyond max_record: a complete-but-untrusted unit,
                    // observed and rejected; nothing was read (no input bytes).
                    let _ = drain_all(&inch, &mut s.in_buf);
                    s.acct.reject_input(0);
                    return 0;
                }
                // A channel fault before a frame is framed is a dependency error,
                // not a received record — it enters no input bucket.
                Admit::ChanError(_) => return 0,
            }
        }
        let frame_in = core::slice::from_raw_parts(frame_ptr, frame_bytes);

        // 4. Resolve the version for this record and thread it through that version's
        //    stages, or emit the fail-closed VERSION_UNAVAILABLE frame.
        // Read the version selector straight from the raw frame — `run_stages`
        // decodes the frame again for stage 0, so a full pre-decode just to find
        // field 255 is duplicate hot-path work.
        let selector = version_selector_from_frame(frame_in);
        let vbin = core::slice::from_raw_parts(s.vbin.as_ptr(), s.vbin_len as usize);
        // No version table is a node of codecs alone: zero stages, so the record
        // passes through them unchanged. A record that names a version still
        // fails closed, since this instance holds none.
        let prog: Option<&[u8]> = if vbin.is_empty() {
            selector.is_empty().then_some(&[0u8][..])
        } else {
            parse_version_table(vbin)
                .and_then(|t| t.resolve(selector).and_then(|i| t.entry(i).map(|e| e.prog)))
        };

        let out_len = match prog {
            Some(prog) => {
                let ns = stage_count(prog);
                if ns > MAX_NODE_STAGES {
                    // Reject an over-cap stage container rather than silently skipping
                    // the trailing stages: a valid container that declares more
                    // stages than this build runs is an error, not a truncation.
                    record_failed(s, sys);
                    0
                } else {
                    let mut stages = [Stage {
                        code: &[],
                        max_cost: 0,
                        on_failure: None,
                        kind: STAGE_KIND_COMPUTE,
                    }; MAX_NODE_STAGES];
                    let mut ok = true;
                    for (i, st) in stages.iter_mut().enumerate().take(ns) {
                        match stage_at(prog, i) {
                            Some(stage) => *st = stage,
                            None => ok = false,
                        }
                    }
                    // Kinds are declared alongside the container, not in it,
                    // so an absent or short vector leaves stages COMPUTE.
                    for (i, st) in stages.iter_mut().enumerate().take(ns) {
                        if i < s.stage_kinds_len as usize {
                            st.kind = s.stage_kinds[i];
                        }
                    }
                    if !ok {
                        record_failed(s, sys);
                        0
                    } else {
                        {
                            let mut spent = 0u64;
                            let r = run_stages_with(
                                &KindedEval,
                                &stages[..ns],
                                frame_in,
                                &mut s.buf_a,
                                &mut s.buf_b,
                                &mut s.out_buf,
                                &mut spent,
                            );
                            s.acct.add_work(spent);
                            r.unwrap_or_else(|e| {
                                if e.over_bound() {
                                    s.over_bound = s.over_bound.wrapping_add(1);
                                }
                                record_failed(s, sys);
                                0
                            })
                        }
                    }
                }
            }
            None => {
                // Fail closed: a record pinned to a version this instance does not hold
                // gets a deterministic {1: "VERSION_UNAVAILABLE"} record (the LB retries
                // another instance) — never the wrong version.
                s.unavailable = s.unavailable.wrapping_add(1);
                write_unavailable_frame(&mut s.out_buf)
            }
        };
        if out_len == 0 {
            return 0;
        }

        // 5. The record's one defined output, by role: a request on
        //    `request_out`, an answer on `response_out`, or a frame on
        //    `result_out`. Immediate delivery resolves the record now; a
        //    retained output leaves it in flight until it drains.
        // A collected or relayed request whose stages named a status is
        // answered here with it rather than passed on: a stage refuses by
        // saying how.
        if s.req_in_chan >= 0 && !s.answer && names_status(s, out_len) {
            emit_answer(s, sys, out_len, false);
            return 0;
        }
        if s.req_out_chan >= 0 {
            emit_request(s, sys, out_len, relay_req);
            return 0;
        }
        if s.answer {
            emit_answer(s, sys, out_len, true);
            return 0;
        }

        // Optional trailing encoder → a wire payload in `enc_out`, copied into
        // `out_buf` so delivery always retries from one stable buffer.
        let final_len = if s.enc_len > 0 {
            match encode_body(s, out_len) {
                Some(m) => {
                    for (d, x) in s.out_buf.iter_mut().zip(s.enc_out.iter()).take(m) {
                        *d = *x;
                    }
                    m
                }
                None => {
                    record_failed(s, sys);
                    return 0;
                }
            }
        } else {
            out_len
        };
        match s.pending.stage(&outch, &s.out_buf, final_len) {
            Staged::Delivered => {
                s.acct.output_delivered_now(final_len as u32);
                record_succeeded(s, sys);
            }
            Staged::Pending => s.acct.output_staged(final_len as u32),
            Staged::Failed(_) => {
                s.pending = Pending { off: 0, len: 0 };
                s.acct.output_failed_now();
                record_failed(s, sys);
            }
        }
        0
    }
}

/// The result record in `out_buf` as a request on `request_out`.
///
/// A relay rewrites the HEAD it took — its id from the mapped field, its flags
/// and inline body as they arrived — and the provider answers the original
/// requester. A requester opens an exchange of its own: the record's carry is
/// kept in the in-flight slot (refused when it does not fit), the body is the
/// `encode` program's output or the record frame itself, and the request is
/// kept for a replay.
#[inline(never)]
unsafe fn emit_request(s: &mut ModuleState, sys: &SyscallTable, out_len: usize, relay: bool) {
    let mut fields = [Field {
        number: 0,
        value: Value::Null,
    }; MAX_PIPE_FIELDS];
    let frame = core::slice::from_raw_parts(s.out_buf.as_ptr(), out_len);
    let Ok(nf) = decode_frame(frame, &mut fields) else {
        record_failed(s, sys);
        return;
    };
    let fields = &fields[..nf];
    let map = s.map;
    if relay {
        let Ok(id) = record_id(&map, fields) else {
            record_failed(s, sys);
            return;
        };
        let at = s.relay_body_at as usize;
        let body = core::slice::from_raw_parts(s.xrec.as_ptr().add(at), s.relay_body_len as usize);
        match request_head(
            &map,
            fields,
            id,
            s.relay_flags,
            s.relay_method,
            s.relay_credit,
            body,
            &mut s.req_buf,
        ) {
            Ok(n) => {
                s.relayed = s.relayed.wrapping_add(1);
                s.acct.output_delivered_now(n as u32);
                s.req_box.send(sys, s.req_out_chan, &s.req_buf, n);
                record_succeeded(s, sys);
            }
            Err(_) => {
                s.acct.output_failed_now();
                record_failed(s, sys);
            }
        }
        return;
    }
    let carry = frame_field_bytes(frame, CARRY_FIELD as u8).unwrap_or(&[]);
    let Some((slot, id)) = s.inflight.reserve(carry) else {
        s.carry_refused = s.carry_refused.wrapping_add(1);
        record_failed(s, sys);
        return;
    };
    let body: &[u8] = if s.enc_len > 0 {
        match encode_body(s, out_len) {
            Some(m) => core::slice::from_raw_parts(s.enc_out.as_ptr(), m),
            None => {
                s.inflight.cancel(slot);
                record_failed(s, sys);
                return;
            }
        }
    } else {
        frame
    };
    let resp_credit = XBODY as u32;
    let method = s.method;
    let built = request_head(
        &map,
        fields,
        id,
        0,
        method,
        resp_credit,
        body,
        s.inflight.request_buf(slot),
    );
    let n = match built {
        Ok(n) => n,
        Err(_) => {
            s.inflight.cancel(slot);
            s.acct.output_failed_now();
            record_failed(s, sys);
            return;
        }
    };
    // With the link down the request is owed, not sent: a provider drops what
    // arrives before its LINK_UP, and `replay_step` sends it after.
    let held = !s.link_up;
    s.inflight.commit(slot, n, held);
    if held {
        s.acct.output_delivered_now(n as u32);
        record_succeeded(s, sys);
        return;
    }
    let req = s.inflight.request_buf(slot);
    for (d, x) in s.req_buf.iter_mut().zip(req.iter()).take(n) {
        *d = *x;
    }
    s.acct.output_delivered_now(n as u32);
    s.req_box.send(sys, s.req_out_chan, &s.req_buf, n);
    record_succeeded(s, sys);
}

/// The result record in `out_buf` as the answer on `response_out`: the
/// exchange its id field names (or the collected request it came from), its
/// status and content type from the mapped fields, its body the `encode`
/// program's output — or, when the record is itself the answer
/// (`record_body`), the record frame; otherwise empty. One HEAD, whole.
#[inline(never)]
unsafe fn emit_answer(s: &mut ModuleState, sys: &SyscallTable, out_len: usize, record_body: bool) {
    let mut fields = [Field {
        number: 0,
        value: Value::Null,
    }; MAX_PIPE_FIELDS];
    let frame = core::slice::from_raw_parts(s.out_buf.as_ptr(), out_len);
    let Ok(nf) = decode_frame(frame, &mut fields) else {
        record_failed(s, sys);
        return;
    };
    let fields = &fields[..nf];
    let map = s.map;
    let id = match record_id(&map, fields) {
        Ok(id) => id,
        Err(_) if s.in_slot >= 0 => s.in_id,
        Err(_) => {
            record_failed(s, sys);
            return;
        }
    };
    let body: &[u8] = if s.enc_len > 0 {
        match encode_body(s, out_len) {
            Some(m) => core::slice::from_raw_parts(s.enc_out.as_ptr(), m),
            None => {
                record_failed(s, sys);
                return;
            }
        }
    } else if record_body {
        frame
    } else {
        &[]
    };
    let ct = core::slice::from_raw_parts(s.content_type.as_ptr(), s.content_type_len as usize);
    match response_head(
        &map,
        fields,
        id,
        exchange::status::OK,
        ct,
        body,
        &mut s.resp_buf,
    ) {
        Ok(n) => {
            s.acct.output_delivered_now(n as u32);
            s.resp_box.send(sys, s.resp_out_chan, &s.resp_buf, n);
            s.in_answered = s.in_slot >= 0;
            record_succeeded(s, sys);
        }
        Err(_) => {
            s.acct.output_failed_now();
            record_failed(s, sys);
        }
    }
}

/// Run the ingress decoder over `inp`, leaving the record frame in `dec_out`.
/// `None` when it cannot become one record — including a payload that builds
/// more fields than the table holds, counted `over_bound`.
#[inline(never)]
unsafe fn decode_into(s: &mut ModuleState, inp: &[u8]) -> Option<usize> {
    let mut b = Builder::new();
    let dec = core::slice::from_raw_parts(s.dec.as_ptr(), s.dec_len as usize);
    let scr = core::slice::from_raw_parts_mut(s.dec_scratch.as_mut_ptr(), DEC_SCRATCH);
    if let Err(e) = eval_decode_scratch(dec, inp, scr, &mut b, 100_000) {
        if e == EvalError::BuildOverflow {
            s.over_bound = s.over_bound.wrapping_add(1);
        }
        return None;
    }
    encode_frame(&b.message(), &mut s.dec_out).ok()
}

/// Run the trailing encoder over the record in `out_buf`, leaving the bytes in
/// `enc_out`. `None` when the record does not decode or the encoder fails —
/// refused, never cut short.
#[inline(never)]
unsafe fn encode_body(s: &mut ModuleState, out_len: usize) -> Option<usize> {
    let mut fields = [Field {
        number: 0,
        value: Value::Null,
    }; MAX_PIPE_FIELDS];
    let enc = core::slice::from_raw_parts(s.enc.as_ptr(), s.enc_len as usize);
    let frame = core::slice::from_raw_parts(s.out_buf.as_ptr(), out_len);
    let nf = decode_frame(frame, &mut fields).ok()?;
    let params = [Message {
        fields: &fields[..nf],
    }];
    eval_bytes(enc, &params, &mut s.enc_out, 100_000).ok()
}

/// Build a fail-closed `{1: "VERSION_UNAVAILABLE"}` record frame into `buf` via the
/// shared `encode_frame` (no hand-poked offsets — one frame encoder). Encoders
/// render field 1, so it surfaces as text; raw consumers see a field-1 marker.
fn write_unavailable_frame(buf: &mut [u8]) -> usize {
    let fields = [Field {
        number: 1,
        value: Value::Bytes(b"VERSION_UNAVAILABLE"),
    }];
    encode_frame(&Message { fields: &fields }, buf).unwrap_or(0)
}
