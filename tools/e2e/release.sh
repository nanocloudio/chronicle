#!/usr/bin/env bash
# THE RELEASE CONTROL PLANE ON DEVICE — running several versions at once, and
# converging a live instance onto a new one:
#
#   chronicle release <default_tag> <tag>:<prog_hex>...   -> the `versions` param
#   chronicle release-ctl add|default|remove <tag> [hex]  -> hot-reload messages
#
# A version is identified by a CONTENT DIGEST (the sha256 prefix of its program),
# not by its tag. That is what makes a mixed-version fleet consistent: the same
# tag resolves to the same bytecode on every instance, so a canary is the same
# code everywhere, and an instance that lacks a pinned version fails closed
# rather than serving the wrong one.
#
# Authoring a release used to require a build host. It does not any more.
. "$(dirname "$0")/../lib.sh"
modules_ready || { no release "fluxor modules build failed"; finish; exit; }

build_graph packaging/cli/linux.yaml || { no release "cli build"; finish; exit; }
cli() { run_text "" 10 -- "$@"; }

# Two distinct programs, so their digests must differ.
BLUE=0102030405
GREEN=0a0b0c0d0e0f

param=$(cli release green "blue:$BLUE" "green:$GREEN" 2>/dev/null | head -1)
case "$param" in
  [0-9a-f]*) ok "device builds a multi-version versions param" ;;
  *) no release "unexpected release output: '$param'" ;;
esac

# `[nvers:u8][default_idx:u8]` opens the param: two versions, default index 1
# (`green` is second). A wrong default index is the failure that silently serves
# the wrong version to everyone.
case "$param" in
  0201*) ok "the param records 2 versions with green as the default" ;;
  *) no release "expected the param to start 0201, got '${param:0:8}'" ;;
esac

# Same content, other default: only the index byte may change.
param_blue=$(cli release blue "blue:$BLUE" "green:$GREEN" 2>/dev/null | head -1)
case "$param_blue" in
  0200*) ok "selecting the other default moves only the default index" ;;
  *) no release "expected 0200, got '${param_blue:0:8}'" ;;
esac
[ "${param:4}" = "${param_blue:4}" ] \
  && ok "the version entries are identical regardless of the default" \
  || no release "entries changed when only the default did"

# Deterministic — a release must be reproducible or the digest could not pin it.
again=$(cli release green "blue:$BLUE" "green:$GREEN" 2>/dev/null | head -1)
[ "$again" = "$param" ] && ok "building the same release twice is deterministic" \
  || no release "second build differed"

# Fail closed on a default that names no version. Without this the param falls
# back to index 0 and quietly serves whichever version happened to be first.
out=$(cli release nosuch "blue:$BLUE" 2>/dev/null)
case "$out" in
  *"default tag names no version"*) ok "an unresolvable default is refused" ;;
  *) no release "expected an unknown-default refusal, got: '$out'" ;;
esac

# Two versions under one tag make the selector ambiguous.
out=$(cli release blue "blue:$BLUE" "blue:$GREEN" 2>/dev/null)
case "$out" in
  *"share a tag"*) ok "duplicate tags are refused" ;;
  *) no release "expected a duplicate-tag refusal, got: '$out'" ;;
esac

# ---- hot reload: converging a RUNNING instance ----

# `add` carries the whole entry (digest, tag, program) behind its opcode 0x01.
add=$(cli release-ctl add green "$GREEN" 2>/dev/null | head -1)
case "$add" in
  01*) ok "release-ctl add emits an ADD_VERSION message" ;;
  *) no release "expected an 01 opcode, got '${add:0:4}'" ;;
esac

# The blue-green flip: opcode 0x02, then the tag length and the tag itself.
def=$(cli release-ctl default green 2>/dev/null | head -1)
# 02 05 "green" = 02 05 677265656e
[ "$def" = "0205677265656e" ] \
  && ok "release-ctl default emits the blue-green flip" \
  || no release "expected 0205677265656e, got '$def'"

# Removing a drained version reclaims its slot: opcode 0x03.
rem=$(cli release-ctl remove blue 2>/dev/null | head -1)
[ "$rem" = "0304626c7565" ] \
  && ok "release-ctl remove emits a REMOVE_VERSION message" \
  || no release "expected 0304626c7565, got '$rem'"

# The digest in an add message is the CONTENT address, so two different programs
# under the same tag must produce different messages. If the tag drove identity,
# a fleet could serve different code under one name.
a1=$(cli release-ctl add v "$BLUE" 2>/dev/null | head -1)
a2=$(cli release-ctl add v "$GREEN" 2>/dev/null | head -1)
[ -n "$a1" ] && [ "$a1" != "$a2" ] \
  && ok "a version's identity follows its program, not its tag" \
  || no release "same message for different programs"

# ---- the closing proof: re-author a param a WORKING graph already runs ----
#
# `examples/multi_version/linux.yaml` carries a hand-pinned `versions` param that
# `multi-version-e2e.sh` drives records through. If the device re-authors it byte
# for byte from the same two programs, then what this command emits is not merely
# well-formed — it is exactly what the pipeline module already consumes,
# digests included.
V1=01ff040000000d0001000202000000400100000041
V2=01ff0600000017000100020200000010020000000000000052400100000041
PINNED=$(sed -n 's/.*versions: "\([0-9a-f]*\)".*/\1/p' examples/multi_version/linux.yaml | head -1)
authored=$(cli release v1 "v1:$V1" "v2:$V2" 2>/dev/null | head -1)
if [ -z "$PINNED" ]; then
  no release "could not read the pinned versions param from the example"
elif [ "$authored" = "$PINNED" ]; then
  ok "the device re-authors the param a running graph already uses"
else
  no release "authored param differs from the pinned one"
fi

out=$(cli release-ctl bogus green 2>/dev/null)
case "$out" in
  *"add|default|remove"*) ok "an unknown control op is a structured error" ;;
  *) no release "expected an unknown-op error, got: '$out'" ;;
esac

finish
