#!/usr/bin/env bash
# AUTHORING DSL on device — reading a `.uproc` MODULE DOCUMENT:
#
#   chronicle parse <uproc_hex>
#
# Reading a whole module document is what lets a device author from source
# rather than assemble artefacts one CLI call at a time.
#
# The parser's structure is held to recorded answers by the uproc corpus in
# `tests/harness/tests/chronicle_cli.rs`. This proves the same parser runs on
# the device runtime, and that a malformed document fails with a POSITION rather
# than a crash — a parser that cannot say where it gave up is not usable for
# authoring.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no uproc "fluxor modules build failed"; finish; exit; }

build_graph packaging/cli/linux.yaml || { no uproc "cli build"; finish; exit; }
cli() { run_text "" 10 -- "$@"; }

hex_of() { python3 -c 'import sys;print(open(sys.argv[1],"rb").read().hex())' "$1"; }

# The real spec module: expressions, a transformation, a decision, a pipeline.
got=$(cli parse "$(hex_of examples/authoring/process_order.uproc)" 2>/dev/null | head -1)
case "$got" in
  "module commerce.process_order "*) ok "device parses the process_order document" ;;
  *) no uproc "process_order: '$got'" ;;
esac
case "$got" in
  *"pipelines=1"*) ok "the pipeline declaration is recognised" ;;
  *) no uproc "pipeline not found in: '$got'" ;;
esac

# The aggregation document.
got=$(cli parse "$(hex_of examples/authoring/customer_totals.uproc)" 2>/dev/null | head -1)
case "$got" in
  *"aggregations=1"*) ok "device parses the customer_totals aggregation" ;;
  *) no uproc "customer_totals: '$got'" ;;
esac

# A malformed document must report WHERE, not just that it failed.
BAD=$(printf 'module a.b {\n  nonsense foo;\n}' | python3 -c 'import sys;print(sys.stdin.buffer.read().hex())')
got=$(cli parse "$BAD" 2>/dev/null | head -1)
case "$got" in
  *"unknown declaration at line 2"*) ok "a malformed document reports its line and column" ;;
  *) no uproc "bad document did not report a position: '$got'" ;;
esac

# Not a document at all.
got=$(cli parse "$(printf 'hello' | python3 -c 'import sys;print(sys.stdin.buffer.read().hex())')" 2>/dev/null | head -1)
case "$got" in
  *"expected a \`module\` header"*) ok "a non-document fails closed" ;;
  *) no uproc "non-document: '$got'" ;;
esac

finish
