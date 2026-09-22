# Structural gates: the register-and-gate pattern

Chronicle's `tools/ci/*.sh` are not tests. A test asserts what the code *does*;
these assert what the tree *is* — properties no amount of passing behaviour would
reveal, because the failure mode is a document quietly ceasing to be true.

This file describes the pattern generically, so another project can implement the
same discipline against its own constants. It is not a library to import; the
gates are deliberately small, dependency-free shell, one file per property, and
copying the shape is cheaper than sharing the code.

## The problem the pattern solves

Every project accumulates facts that live in two places at once: a ceiling that is
both a `const` and a sentence in a design doc, a taxonomy that is both an emission
order and a manifest list, an invariant that is both a rule and a comment. The two
drift. Nothing fails, because nothing checks — and the document becomes actively
harmful, since a reader now trusts a claim the code stopped honouring.

The pattern's premise: **if a property is worth asserting in prose, it is worth a
gate that fails the build when the prose goes stale.**

## Shape 1: a hand-written register, parsed against source

Used by `fluxor ci`'s `limit-register` phase, against
`docs/architecture/limit_register.md`. Not a script here: every project carrying
a register is subject to the same gate, so it lives in the tool rather than being
reimplemented per project.

A *register* is a human-readable document that a machine can also read. It carries
prose — rationale, failure mode, change rule, which tests cover it — because a
bare list of numbers teaches nobody why a bound is what it is. Inside it sits one
fenced block in a trivially parseable format:

```
NAME | path/to/source.rs | expected-right-hand-side | profile
```

The profile field is optional and is what a per-silicon register needs: a
constant declared behind several `cfg` predicates gets one row per profile, and
the gate derives each declaration's profile from the predicates that actually
guard it. Chronicle omits it — see the register's own Profiles section for why —
and fluxor's register carries it on every row.

A second fenced block, `limit-constraints`, records RELATIONSHIPS between limits
and checks each is still the condition of a live `assert!`. Values alone cannot
express "these two must agree", and that is precisely what a retune breaks.

The gate extracts the block, and for each row finds `const NAME ... = <rhs>;` at
the recorded path and compares the right-hand side textually after collapsing
whitespace. Textual comparison is deliberate: it accepts derived values
(`2 * UPROC_BUF`) without evaluating anything, so the register records what the
source says rather than a number that has to be recomputed by hand.

Design rules that make this work:

- **The document is the source of truth for the *list*; the code is the source of
  truth for the *values*.** The gate never rewrites either. A mismatch is a human
  decision: the bound moved deliberately, or it moved by accident.
- **Accept private constants.** A ceiling is observable to whoever hits it,
  whether or not it is `pub`. Matching `const NAME` rather than `pub const NAME`
  keeps visibility out of the rule.
- **State the register's own admission rule in the register.** Chronicle's is:
  a bound that shapes what an artefact may hold or do belongs here, and one found
  in source but absent here is a defect. Without that sentence, "is this row
  missing or out of scope?" has no answer.
- **Name foreign authority explicitly.** Where a constant mirrors another
  project's (Chronicle's `SLOT_SIZE` mirrors Fluxor's `GRAPH_SLOT_SIZE`), the gate
  can only prove the local copy matches the local source. Say so in the row, or a
  reader will believe the gate is checking something it cannot see.

## Shape 2: a generated document with a freshness diff

Used by `resource-summary.sh` (`docs/architecture/resource_summary.md`) and
`hop-count.sh` (`docs/architecture/hop_register.md`).

Where a register is written by hand and checked against source, this shape
derives the whole document from source — manifests, graph wiring — into a
temporary file and diffs it against the committed copy. Identical passes;
different fails and prints the diff. The script defaults to `--check` and
regenerates in place when run with no argument, so the fix for a failure is to
run the gate itself and commit what it writes.

Use this where the document has no editorial content: a per-module table of
record capacities and instrument counts is a projection of the manifests, and a
human retyping it adds only the opportunity to be wrong. Use Shape 1 instead
wherever the document must carry rationale a generator cannot know — why a bound
is what it is, what happens when it is hit, which test proves it.

The distinction matters when choosing: a generated doc can never drift, but it
can also never explain itself.

## Shape 3: an inventory with classified exemptions

Used by `core-coverage.sh`, and by `unsafe-seam.sh`.

Where a register pins values, an inventory pins *membership*: every item in a set
must be classified, and an unclassified item fails. Chronicle's coverage gate puts
each shipping core into one of four buckets — directly tested, transitively
covered, exempt with a reason, or a tracked gap with a reason — and fails on
anything that fits none. The effect is that a **new** core cannot ship without
somebody making a coverage decision, which is the property worth having; the gate
is not trying to measure coverage, it is trying to make silence impossible.

Design rules:

- **Exemptions carry a reason string, in the gate, next to the name.** Not a
  separate document, not a comment above the list — the reason is printed in the
  gate's own output, so reading a passing run tells you what has been forgiven and
  why.
- **A tracked gap is a pass that prints loudly.** Debt that fails the build gets
  deleted or lied about; debt that is invisible never gets paid. A `GAP` bucket
  that prints on every green run is the compromise that survives contact.
- **Check the allowlist as well as the code.** The failure mode of any exemption
  list is that it becomes where things go to be forgiven. `unsafe-seam.sh` asserts
  both directions: nothing outside the list has `unsafe`, *and* everything on the
  list genuinely reaches the syscall table and genuinely still contains `unsafe`.
  An entry that stops being true is a failure, so the list cannot rot.

## Shape 4: a structural assertion about absence

Used by `shipping-surface.sh`, `secret-safety.sh`, `accounting-order.sh`.

Some properties are about what *cannot* happen: the shipping surface reaches no
cargo crate; no metric value or default log message carries a record body; the
baseline instrument names are front-loaded in every manifest, so emission by
index holds.

The rule that makes these gates worth writing: **assert the property directly, not
a proxy for it.** `shipping-surface.sh` checks that no crate directory exists at
all, rather than only checking that modules do not include from one — because the
proxy check goes quietly vacuous the moment the directory is absent, and a vacuous
gate is worse than no gate.

## What a failure must say

A gate's output is read by someone who did not write it, usually while they are
trying to do something else. Chronicle's convention is one indented
`  PASS  <property>` or `  FAIL  <gate>: <what and where>` line per assertion, so
a CI log reads the same whichever gate produced it. Gates that make several
assertions can take the `ok`/`no`/`finish` helpers from `tools/lib.sh` for the
tally; a single-assertion gate manages its own exit status. Either way a failure
names:

1. the specific thing that broke (file, constant, name — not "a check failed");
2. the value found and the value expected, when both exist;
3. **the action** — what to edit to make it pass, including the legitimate escape
   hatch where one exists ("add it to SEAM with the reason it must reach the
   table").

The third is what separates a gate people maintain from one they delete. A gate
that says "unclassified" and stops is a puzzle; one that names the four buckets
and where to declare them is a decision.

## Porting this

1. Pick one property currently asserted only in prose.
2. Decide which shape it is: a value that can drift (register), a document that
   is a pure projection of source (generate and diff), a set that can grow
   silently (inventory), or something that must never appear (absence).
3. Write the smallest shell that fails when the prose goes stale, and make its
   failure message name the fix.
4. Register it in the project's CI script list so it runs unattended.

Do not start by building shared tooling. Two projects with the same property will
disagree about its shape long before they disagree about its implementation, and a
50-line gate that fits one project exactly is worth more than a framework that
fits neither.
