// The expression module's record-lifecycle STEP, kept out of the ABI shell so
// it can be driven by a scripted `Chan` in the host harness. The
// `.fmod` `module_step` is a thin adapter: build the production `SysChan`s and
// call `expr_step`. All domain sequencing — drain-before-admit, whole-frame
// admission, decode/eval, output_too_large rejection, retained delivery — lives
// here in safe code over the `io_core` seam, so its no-loss behaviour under forced
// EAGAIN is a host test rather than a claim about an unsafe function.
//
// Mounted after vm_core, pipeline_core, outcome_core and io_core (it uses
// `decode_frame`/`frame_len`/`eval_scratch`/`Scratch`/`Field`/`Message`/`Value`,
// the `io_core` lifecycle and `outcome_core::Reason`).

// `StepResult` is shared from io_core (expression never returns `Dropped`).

/// One step of the expression lifecycle. `code`/`max_cost` are the loaded program;
/// `in_buf`/`out_buf` are the module's max_record buffers; `pending` is its single
/// retained-output cursor. Drains any pending output first (a module does not read
/// while output is pending), otherwise admits at most one whole typed frame,
/// evaluates the program over its fields, and stages the scalar result.
#[allow(
    clippy::too_many_arguments,
    reason = "the module's state fields, passed \
    explicitly so the core stays a pure function over the io_core seam rather than \
    reaching into an ABI struct"
)]
pub fn expr_step(
    inch: &impl Chan,
    outch: &impl Chan,
    in_buf: &mut [u8],
    out_buf: &mut [u8],
    pending: &mut Pending,
    code: &[u8],
    max_cost: u64,
    // Accounting: the disposition of every observed record is recorded here,
    // at the one point that knows admit-bytes and whether a delivery completed a
    // fresh admission or drained a retained output. The invariants hold by
    // construction.
    acct: &mut Accounting,
) -> StepResult {
    // 1. Deliver any retained output before admitting new input. The
    //    retained frame belongs to an already-admitted, still-in-flight input;
    //    its full delivery is what completes that input.
    if !pending.is_empty() {
        let plen = pending.len as u32;
        return match pending.drain(outch, out_buf) {
            Staged::Delivered => {
                acct.output_drained(plen);
                acct.input_succeeded();
                StepResult::Delivered
            }
            Staged::Pending => StepResult::Pending,
            Staged::Failed(r) => {
                *pending = Pending { off: 0, len: 0 };
                acct.output_failed_pending(plen);
                acct.input_failed();
                StepResult::Failed(r)
            }
        };
    }

    // 2. Admit exactly one whole typed frame, non-destructively.
    let read = match admit_frame(inch, in_buf, frame_len) {
        Admit::Complete(n) => {
            acct.admit_input(n as u64);
            n
        }
        Admit::Empty | Admit::NeedMore => return StepResult::Idle,
        Admit::BoundaryLost => {
            // A frame beyond max_record makes the stream boundary untrustworthy;
            // reset the channel fail-closed rather than desync the next frame. A
            // complete-but-untrusted unit was observed and rejected; nothing was
            // read, so it contributes no input bytes.
            let _ = drain_all(inch, in_buf);
            acct.reject_input(0);
            return StepResult::Rejected(Reason::TooLarge);
        }
        // A channel fault before any frame was framed is a dependency error, not a
        // received record — it enters no input bucket.
        Admit::ChanError(r) => return StepResult::Failed(r),
    };

    // 3. Decode → evaluate → write the scalar result into out_buf. A malformed frame
    //    (trusted boundary, bad content) is a terminal input failure; an oversized
    //    result is rejected as output_too_large — never clipped. in_buf and
    //    out_buf are distinct slices, so the shared decode borrow and the result
    //    write do not alias.
    let res_len: usize = {
        let mut fields = [Field {
            number: 0,
            value: Value::Null,
        }; MAX_PIPE_FIELDS];
        let nfields = match decode_frame(&in_buf[..read], &mut fields) {
            Ok(c) => c,
            Err(_) => {
                acct.input_failed();
                return StepResult::Failed(Reason::Malformed);
            }
        };
        let params = [Message {
            fields: &fields[..nfields],
        }];
        let mut sbuf = [0u8; 512];
        let mut scratch = Scratch::new(&mut sbuf);
        // Meter the VM instructions this record spent (work units) — recorded
        // even on an eval fault below, since a failed program still consumed work.
        let mut spent = 0u64;
        let ev = eval_scratch_metered(code, &params, &mut scratch, max_cost, &mut spent);
        acct.add_work(spent);
        match ev {
            Ok(v) => match resolve_scratch(v, &scratch) {
                Value::Bytes(result) => {
                    if result.len() > out_buf.len() {
                        acct.input_failed();
                        return StepResult::Failed(Reason::TooLarge);
                    }
                    out_buf[..result.len()].copy_from_slice(result);
                    result.len()
                }
                _ => {
                    acct.input_failed();
                    return StepResult::Failed(Reason::Unsupported);
                }
            },
            Err(_) => {
                acct.input_failed();
                return StepResult::Failed(Reason::CostExceeded);
            }
        }
    };

    // 4. Stage the result; retain it on a full ring. Immediate delivery completes
    //    the input now; a retained output leaves it in flight until it drains.
    match pending.stage(outch, out_buf, res_len) {
        Staged::Delivered => {
            acct.output_delivered_now(res_len as u32);
            acct.input_succeeded();
            StepResult::Delivered
        }
        Staged::Pending => {
            acct.output_staged(res_len as u32);
            StepResult::Pending
        }
        Staged::Failed(r) => {
            *pending = Pending { off: 0, len: 0 };
            acct.output_failed_now();
            acct.input_failed();
            StepResult::Failed(r)
        }
    }
}
