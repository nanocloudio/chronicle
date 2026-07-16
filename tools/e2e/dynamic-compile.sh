#!/usr/bin/env bash
# THE DYNAMIC DELIVERY PATH ON DEVICE — "POST source, no build step":
#
#   chronicle compile-source <schema> <param> <type> <source>   -> ir hex
#   chronicle compile-stages <schema> <param> <type> <src>...   -> ir_stages hex
#
# Distinct from `author`/`graph`, which compile a `.uproc` DOCUMENT. Here a node
# holds a schema and type-checks source it has never seen, handed over at
# runtime. The result is the same shippable checked IR the modules lower at load,
# so the target proves it can run it by lowering it — no opaque bytecode, and no
# out-of-band agreement on the opcode set.
#
# The rejections matter as much as the successes: source that type-checks but
# does not construct a message packs into a well-formed container and fails
# inside the VM at runtime, so it has to be refused here.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no dynamic "fluxor modules build failed"; finish; exit; }

build_graph packaging/cli/linux.yaml || { no dynamic "cli build"; finish; exit; }
cli() { run_text "" 10 -- "$@"; }

SCHEMA='Order{customer_id:str@2,total:int@3};Receipt{id:str@1,amount:int@2};'

# A scalar expression compiles and is REPORTED as a scalar — the caller needs to
# know, because only a message-constructing program is a valid pipeline stage.
out=$(cli compile-source "$SCHEMA" order Order 'order.total + 10' 2>/dev/null)
ir=$(printf '%s\n' "$out" | head -1)
kind=$(printf '%s\n' "$out" | sed -n 2p)
case "$ir$kind" in
  [0-9a-f]*scalar) ok "a scalar expression compiles at runtime and is reported as scalar" ;;
  *) no dynamic "unexpected compile-source output: '$out'" ;;
esac

# A message-constructing expression is reported as such.
out=$(cli compile-source "$SCHEMA" order Order 'Receipt{id: order.customer_id, amount: order.total}' 2>/dev/null)
[ "$(printf '%s\n' "$out" | sed -n 2p)" = "message" ] \
  && ok "a message-constructing expression is reported as a message" \
  || no dynamic "expected 'message', got: '$(printf '%s\n' "$out" | sed -n 2p)'"

# Deterministic: the same source and schema must yield the same IR, or the
# result could not be content-addressed.
a=$(cli compile-source "$SCHEMA" order Order 'order.total + 10' 2>/dev/null | head -1)
b=$(cli compile-source "$SCHEMA" order Order 'order.total + 10' 2>/dev/null | head -1)
[ -n "$a" ] && [ "$a" = "$b" ] \
  && ok "compiling the same source twice is deterministic" \
  || no dynamic "non-deterministic compile: '$a' vs '$b'"

# Fail closed on a bad field. A node accepting source from outside must reject
# it structurally, never emit a program that reads a field that isn't there.
out=$(cli compile-source "$SCHEMA" order Order 'order.nope' 2>/dev/null)
case "$out" in
  *"did not type-check"*) ok "an unknown field is refused" ;;
  *) no dynamic "expected a type-check refusal, got: '$out'" ;;
esac

# ...and on an unknown message type.
out=$(cli compile-source "$SCHEMA" order Missing 'order.customer_id' 2>/dev/null)
case "$out" in
  *"did not type-check"*) ok "an unknown input type is refused" ;;
  *) no dynamic "expected a type-check refusal, got: '$out'" ;;
esac

# A chain of stages becomes the ir_stages container the pipeline module loads.
stages=$(cli compile-stages "$SCHEMA" order Order \
  'Receipt{id: order.customer_id, amount: order.total}' 2>/dev/null | head -1)
case "$stages" in
  [0-9a-f]*) ok "a stage chain compiles to an ir_stages container" ;;
  *) no dynamic "unexpected compile-stages output: '$stages'" ;;
esac

# The container must be one the device can actually LOWER — "it lowered" is the
# target's proof it can run it, and is exactly what `check` performs.
out=$(cli check "$stages" 2>/dev/null)
case "$out" in
  *error*|"") no dynamic "the compiled container did not lower: '$out'" ;;
  *) ok "the compiled container lowers on device" ;;
esac

# A scalar stage is refused: the executor hands one stage's output frame to the
# next, so a scalar has nothing to pass on.
out=$(cli compile-stages "$SCHEMA" order Order 'order.total' 2>/dev/null)
case "$out" in
  *"must construct a message"*) ok "a non-message stage is refused" ;;
  *) no dynamic "expected a non-message refusal, got: '$out'" ;;
esac

finish
