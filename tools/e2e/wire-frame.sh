#!/usr/bin/env bash
# REGION FRAMING, live: a pipeline's `encode` nests a u32 length region around
# a crc32c region around a varint-length header, and the built `.fmod` must emit
# the envelope byte for byte. The host suite proves each region kind on its own
# (`tests/harness/tests/pipeline_suites/ser.rs`); this proves them composed, on
# the runtime, from the param the packer hands the module.
#
# {1: 258, 2: "hi"} -> [len 16][crc32c][varint 9][02][258 as i64 BE]["hi"]
. "$(dirname "$0")/../lib.sh"
modules_ready || { no frame "fluxor modules build failed"; finish; exit; }

build_graph examples/wire_frame/linux.yaml || { no frame "graph build"; finish; exit; }

got=$(run_hex 02010108000201000000000000020002006869 4)
[ "$got" = "0000001029aa79ca090200000000000001026869" ] \
  && ok "wire frame (u32 length ⊃ crc32c ⊃ varint header, byte-exact)" \
  || no frame "got=$got"

finish
