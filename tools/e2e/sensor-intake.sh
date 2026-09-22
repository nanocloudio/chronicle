#!/usr/bin/env bash
# THE SENSOR SURFACE, END TO END — a reading crosses it and is decided on.
#
# `temp_sensor` (fluxor) publishes the `sensor.sample` capability and emits a
# 24-byte `SensorSample`; `sensor_intake` translates that into the chronicle
# record frame; `decision` evaluates compiled first-hit rules over it. On an
# RP2040 that chain is `examples/sensor_rules/pico.yaml`, which cannot be
# executed without RP2040 silicon.
#
# What CAN be executed is the translation and the evaluation, and this drives
# them on the host: a real `SensorSample` in, the decided outcome out. The point
# is not "it produced something" — it is that the i64 the adapter WROTE is the i64
# the VM READ, at the field number the rules address, with the sign intact. A
# translation that put the value one field over, or truncated the timestamp into
# it, would still produce a plausible-looking frame.
#
# Both arms of the policy are driven, because an adapter that emitted a constant
# would agree with the default arm by accident.
#
# `SensorSample` layout (little-endian): [sensor_id u16][flags u8][scale i8]
#                                       [seq u32][t_micros u64][value i64]
. "$(dirname "$0")/../lib.sh"
modules_ready || { no sensor "fluxor modules build failed"; finish; exit; }

build_graph examples/sensor_rules/linux.yaml || { no sensor "graph build"; finish; exit; }

# One u64/i64 as little-endian hex — the wire order every field uses.
le64() {
  local v=$1 out="" i
  for i in 0 1 2 3 4 5 6 7; do
    out+=$(printf '%02x' $(((v >> (8 * i)) & 0xff)))
  done
  printf '%s' "$out"
}

# sensor_id=1, flags=0, scale=-3 (millidegrees), seq=7, t_micros=0, value=<v>.
# The first eight bytes are the fixed header: 01 00 | 00 | fd | 07 00 00 00.
sample() { printf '010000fd07000000%s%s' "$(le64 0)" "$(le64 "$1")"; }

run_case() {
  local value=$1 want=$2 label=$3
  local got; got=$(run_hex "$(sample "$value")" 8)
  if [ -z "$got" ]; then
    no sensor "$label: produced nothing"
  elif [ "$got" != "$want" ]; then
    no sensor "$label: produced '$got', expected '$want'"
  else
    ok "sensor surface $label (value $value → $want)"
  fi
}

# The decision routes field 1 (`value`) > 1000 → {1:100}, else → {1:0}.
# 23500 is 23.5 °C in millidegrees — an ordinary room reading, and above the
# threshold; 500 is below it and takes the default.
run_case 23500 01010108006400000000000000 "rule fires on a warm reading"
run_case 500   01010108000000000000000000 "default fires on a cold reading"

finish
