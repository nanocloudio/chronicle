// The decision module's record-lifecycle STEP, kept out of the ABI shell so it
// can be host-driven by a scripted `Chan`. Same discipline as
// `expression_step_core`, plus the decision DROP convention: a first-hit policy
// whose matched outcome constructs an empty message is a deliberate filter, a
// terminal counted `Dropped` disposition — distinct from output pressure and from
// a processing failure.
//
// Mounted after vm_core, pipeline_core, decision_core, outcome_core and io_core.

/// One step of the decision lifecycle. `cont` is the loaded, load-scanned decision
/// container. Drains pending output first, otherwise admits one whole typed frame,
/// runs the first-hit policy, and stages the constructed outcome — or records a
/// policy drop when the outcome is empty.
#[allow(
    clippy::too_many_arguments,
    reason = "the module's state fields plus the audit out-param, passed explicitly so \
    the core stays a pure function over the io_core seam"
)]
pub fn decision_step(
    inch: &impl Chan,
    outch: &impl Chan,
    in_buf: &mut [u8],
    out_buf: &mut [u8],
    pending: &mut Pending,
    cont: &[u8],
    // Audit out-param: set to the matched rule index (0..) when a rule fired,
    // `-1` when the DEFAULT fired, and left unchanged when no decision ran this step.
    fired: &mut i16,
    // Accounting: every observed record classified into one input/output
    // disposition at the point that knows admit-bytes and drain-vs-fresh.
    acct: &mut Accounting,
) -> StepResult {
    // 1. Deliver any retained output before admitting new input.
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

    // 2. Admit exactly one whole typed frame.
    let read = match admit_frame(inch, in_buf, frame_len) {
        Admit::Complete(n) => {
            acct.admit_input(n as u64);
            n
        }
        Admit::Empty | Admit::NeedMore => return StepResult::Idle,
        Admit::BoundaryLost => {
            let _ = drain_all(inch, in_buf);
            acct.reject_input(0);
            return StepResult::Rejected(Reason::TooLarge);
        }
        // A channel fault before a frame is framed is a dependency error, not a
        // received record.
        Admit::ChanError(r) => return StepResult::Failed(r),
    };

    // 3. Decode → run the first-hit policy → construct the outcome. in_buf (shared,
    //    via fields) and out_buf (mut) are distinct slices, so encoding the outcome
    //    while the input is borrowed does not alias.
    let encoded: usize = {
        let mut fields = [Field {
            number: 0,
            value: Value::Null,
        }; MAX_PIPE_FIELDS];
        let nf = match decode_frame(&in_buf[..read], &mut fields) {
            Ok(nf) => nf,
            Err(_) => {
                acct.input_failed();
                return StepResult::Failed(Reason::Malformed);
            }
        };
        let params = [Message {
            fields: &fields[..nf],
        }];
        let mut builder = Builder::new();
        let mut sbuf = [0u8; STAGE_SCRATCH_CAP];
        let mut scratch = Scratch::new(&mut sbuf);
        // A decision-run failure is a terminal processing failure (counted, not
        // lost). On success, record WHICH branch produced the outcome for the
        // audit: a rule index, or -1 for the default.
        let mut spent = 0u64;
        let outcome =
            run_decision_scratch_metered(cont, &params, &mut builder, &mut scratch, &mut spent);
        acct.add_work(spent);
        match outcome {
            Ok(Fired::Rule(i)) => *fired = i as i16,
            Ok(Fired::Default) => *fired = -1,
            Err(_) => {
                acct.input_failed();
                return StepResult::Failed(Reason::Internal);
            }
        }
        // DROP convention: an empty constructed outcome routes nowhere — a
        // deliberate zero-output filter, terminal for the admitted input.
        if builder.message().fields.is_empty() {
            acct.input_policy_dropped();
            return StepResult::Dropped;
        }
        match encode_frame_scratch(&builder.message(), &scratch, out_buf) {
            Ok(m) => m,
            // The outcome does not fit a max_record frame: rejected, never clipped.
            Err(_) => {
                acct.input_failed();
                return StepResult::Failed(Reason::TooLarge);
            }
        }
    };

    // 4. Stage the outcome; retain it on a full ring.
    match pending.stage(outch, out_buf, encoded) {
        Staged::Delivered => {
            acct.output_delivered_now(encoded as u32);
            acct.input_succeeded();
            StepResult::Delivered
        }
        Staged::Pending => {
            acct.output_staged(encoded as u32);
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
