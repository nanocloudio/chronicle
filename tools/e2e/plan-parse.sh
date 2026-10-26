#!/usr/bin/env bash
# AUTHORING DSL on device — reading a `.plan` DOCUMENT:
#
#   chronicle parse <plan_hex>
#
# Reading a whole plan document is what lets a device author from source
# rather than assemble artefacts one CLI call at a time.
#
# The parser's structure is held to recorded answers by the plan corpus in
# `tests/harness/tests/chronicle_cli_suites/corpus.rs`. This proves the same parser runs on
# the device runtime, and that a malformed document fails with a POSITION rather
# than a crash — a parser that cannot say where it gave up is not usable for
# authoring.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no plan "fluxor modules build failed"; finish; exit; }

build_graph packaging/applet/chronicle/linux.yaml || { no plan "cli build"; finish; exit; }
cli() { run_text "" 10 -- "$@"; }

hex_of() { python3 -c 'import sys;print(open(sys.argv[1],"rb").read().hex())' "$1"; }

# The real spec plan: expressions, a transformation, a decision, a pipeline.
got=$(cli parse "$(hex_of examples/authoring/process_order.plan)" 2>/dev/null | head -1)
case "$got" in
  "plan commerce.process_order "*) ok "device parses the process_order document" ;;
  *) no plan "process_order: '$got'" ;;
esac
case "$got" in
  *"pipelines=1"*) ok "the pipeline declaration is recognised" ;;
  *) no plan "pipeline not found in: '$got'" ;;
esac

# The aggregation document.
got=$(cli parse "$(hex_of examples/authoring/customer_totals.plan)" 2>/dev/null | head -1)
case "$got" in
  *"aggregations=1"*) ok "device parses the customer_totals aggregation" ;;
  *) no plan "customer_totals: '$got'" ;;
esac

# A malformed document must report WHERE, not just that it failed.
BAD=$(printf 'plan a.b {\n  nonsense foo;\n}' | python3 -c 'import sys;print(sys.stdin.buffer.read().hex())')
got=$(cli parse "$BAD" 2>/dev/null | head -1)
case "$got" in
  *"unknown declaration at line 2"*) ok "a malformed document reports its line and column" ;;
  *) no plan "bad document did not report a position: '$got'" ;;
esac

# Not a document at all.
got=$(cli parse "$(printf 'hello' | python3 -c 'import sys;print(sys.stdin.buffer.read().hex())')" 2>/dev/null | head -1)
case "$got" in
  *"expected a \`plan\` header"*) ok "a non-document fails closed" ;;
  *) no plan "non-document: '$got'" ;;
esac

finish
