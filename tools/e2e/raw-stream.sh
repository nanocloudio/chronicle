#!/usr/bin/env bash
# RAW-STREAM FRAMING, live: a pipeline's `decode` reads a byte STREAM on
# `record_in`, and the records it decodes must not depend on how the stream was
# cut into reads. Two records in one write, and one record split over two
# writes, through the built `.fmod` on the runtime. The host suite proves the
# framing rules case by case (`tests/harness/tests/pipeline_suites/deser.rs`,
# `rd_frame`); this proves the module holds bytes across reads.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no raw "fluxor modules build failed"; finish; exit; }

# feed <secs> <hex>…: each hex chunk written separately, a pause between, so a
# chunk reaches the graph as its own read.
feed() {
  local secs=$1 c
  shift
  {
    for c in "$@"; do
      printf '%s' "$c" | xxd -r -p
      sleep 1
    done
  } | timeout -k 2 "$secs" "$FL_BIN" --config "$CFG" --modules "$MODS" 2>/dev/null
}

# ── decode_len: fixed-width records (u32 LE amount + 4 bytes padding) ───────
build_graph examples/raw_stream/fixed.yaml || { no raw "fixed build"; finish; exit; }

got=$(feed 4 070000007061642e090000007061642e)
[ "$got" = $'7\n9' ] && ok "two fixed-width records in one read both decode, in order" \
  || no raw "fixed, one read: got '$got'"

got=$(feed 5 070000 007061642e09 0000007061642e)
[ "$got" = $'7\n9' ] && ok "a fixed-width record split over two reads is reassembled" \
  || no raw "fixed, split: got '$got'"

# ── the program frames: 'R' [len:u8] [bytes] ────────────────────────────────
build_graph examples/raw_stream/framed.yaml || { no raw "framed build"; finish; exit; }

got=$(feed 4 520261625203636465)
[ "$got" = $'ab\ncde' ] && ok "several self-framed records in one read all decode, in order" \
  || no raw "framed, one read: got '$got'"

got=$(feed 5 52056865 6c6c6f5201 7a)
[ "$got" = $'hello\nz' ] && ok "a self-framed record split across reads is reassembled" \
  || no raw "framed, split: got '$got'"

got=$(feed 4 78797a520161)
[ "$got" = "a" ] && ok "bytes that start no record are resynchronised past" \
  || no raw "framed, garbage: got '$got'"

finish
