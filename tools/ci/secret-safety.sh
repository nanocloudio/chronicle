#!/usr/bin/env bash
# Secret-safety scan: no telemetry METRIC value and no
# default LOG message may carry a record body, user key, token, or other
# unbounded/secret value. Chronicle's metrics are numeric counters/gauges and its
# logs are static fault strings BY CONSTRUCTION; this structural check guards
# against a regression that pipes record/parameter data into either — the kind of
# leak that turns an observability line into an exfiltration channel.
set -u
fail=0
mods="modules/app/expression/mod.rs modules/app/decision/mod.rs \
      modules/app/pipeline/mod.rs modules/app/aggregation/mod.rs \
      modules/app/chronicle_cli/mod.rs"

# Record / parameter DATA buffers this module family holds. A log message or a
# metric value that references one of these is carrying content, not a count.
databufs='in_buf|out_buf|buf_a|buf_b|hex|cont|record|frame|emit_q|ckpt_stage|state_hex|dec_out|enc_out|arec|ir_scratch|vbin|prog'

# 1. A dev_log whose message pointer is a data buffer rather than a static string.
if grep -rEn "dev_log\([^,]*,[^,]*,[[:space:]]*s\.($databufs)" $mods 2>/dev/null; then
  echo "  FAIL  secret-safety: a dev_log passes a record/param data buffer as its message"
  fail=1
fi

# 2. A telemetry value that references a data buffer rather than a numeric field.
if grep -rEn "tlm_(counter|gauge)\([^)]*s\.($databufs)\b" $mods 2>/dev/null; then
  echo "  FAIL  secret-safety: a telemetry value references a record/param data buffer"
  fail=1
fi

if [ "$fail" -eq 0 ]; then
  echo "  PASS  secret-safety: no record/param data reaches a log message or metric value"
fi
exit $fail
