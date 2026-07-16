#!/usr/bin/env bash
# THE SHIPPING SURFACE CARRIES NO CARGO DEPENDENCY.
#
# What ships is `.fmod` PIC modules built from `modules/common/*_core.rs` and the
# fluxor SDK. A device running them needs no cargo, no crates, and no Linux build
# host — that is the whole point of the project.
#
# There is no longer any host code at all: the differential oracles were retired
# into golden corpora, each beside the harness that reads it, and the last crate
# is gone. This gate keeps it that way — the cheapest way to lose the property is for someone
# to add a crate "just for a helper" and start including from it.
#
# Deliberately structural, not behavioural: it asserts what the runtime CANNOT
# reach, which no amount of passing tests would reveal.
. "$(dirname "$0")/../lib.sh"

# 1. There is no cargo crate in this project. Asserted directly rather than
#    inferred: a gate that only checks "modules do not include from crates/" goes
#    quietly vacuous the moment the directory is absent.
[ ! -d crates ] \
  && ok "no cargo crate exists in the project" \
  || no surface "crates/ is back: $(ls crates 2>/dev/null | tr '\n' ' ')"

#    Module SOURCES only: `modules/*/*/tests/` are harnesses, not part of a .fmod.
msrc() { find modules -name '*.rs' -not -path '*/tests/*'; }
hits=$(msrc | xargs grep -hn 'include!' 2>/dev/null | grep -c 'crates/' || true)
[ "$hits" = "0" ] \
  && ok "no module includes host code from a crate" \
  || no surface "$hits module include!(s) reach into crates/"

# 2. Modules may only include from `modules/common` (the shared cores) or
#    `target/fluxor` (the SDK). Anything else is a new, unreviewed path in.
roots=$(msrc | xargs grep -hoE 'include!\("[^"]+"' 2>/dev/null \
  | sed 's/.*include!("//' | sed 's|^\(\.\./\)*||' | cut -d/ -f1 | sort -u | tr '\n' ' ')
case "$(printf '%s' "$roots" | tr -s ' ')" in
  "common target "|"target common ") ok "modules include only shared cores and the fluxor SDK" ;;
  *) no surface "unexpected include roots: '$roots'" ;;
esac

# 3. Nothing anywhere builds a cargo binary into the delivery path.
bins=$(find . -name Cargo.toml -not -path './target/*' -exec grep -l '\[\[bin\]\]' {} + 2>/dev/null | wc -l)
[ "$bins" = "0" ] \
  && ok "nothing builds a shippable cargo binary" \
  || no surface "$bins manifest(s) declare [[bin]]"

# 4. No e2e script may shell out to cargo. Scripts drive the built artefacts; a
#    `cargo run` here would mean the proof depended on a host toolchain, and the
#    e2e suite would stop being evidence about the device. Comment lines are
#    allowed, and this gate excludes itself since it names what it forbids.
execs=$(grep -rn 'cargo run' tools/ci/*.sh tools/e2e/*.sh tools/lib.sh 2>/dev/null \
  | grep -v 'shipping-surface' | grep -vc '^[^:]*:[0-9]*: *#' || true)
[ "$execs" = "0" ] \
  && ok "no e2e script executes a crate" \
  || no surface "$execs e2e script line(s) run cargo"

# 5. `modules/common/` holds shipping cores and nothing else. Test fixtures once
#    lived here, which made "what ships" unreadable from the tree: 9.5 MB of
#    golden corpora sat beside the PIC sources they had nothing to do with. They
#    now live beside the single harness that reads each one.
strays=$(find modules/common -type f -not -name '*.rs' | wc -l)
[ "$strays" = "0" ] \
  && ok "modules/common holds only shipping sources" \
  || no surface "$strays non-source file(s) under modules/common: $(find modules/common -type f -not -name '*.rs' | head -3 | tr '\n' ' ')"

# 6. The artefacts really are built. A gate over an empty target/ would pass
#    while proving nothing at all.
built=$(find target/fluxor -name 'chronicle_cli.fmod' 2>/dev/null | wc -l)
[ "$built" -ge 1 ] \
  && ok "the authoring module is built as a .fmod artefact" \
  || no surface "no chronicle_cli.fmod found — run 'fluxor modules build --all'"

# 7. ...and it is a real PIC object, not a stub. The whole authoring chain lives
#    in this one module, so a truncated build would pass every check above.
sz=$(find target/fluxor -name 'chronicle_cli.fmod' -printf '%s\n' 2>/dev/null | sort -rn | head -1)
[ "${sz:-0}" -gt 4096 ] \
  && ok "the authoring module is a substantive artefact (${sz} bytes)" \
  || no surface "chronicle_cli.fmod is ${sz:-0} bytes — suspiciously small"

finish
