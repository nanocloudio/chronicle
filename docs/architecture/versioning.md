# Multi-Version Modules

## Overview

A single pipeline instance can hold several versions of its program at once and
choose one **per record** — by default, or pinned by an `X-Module-Version` tag. A
version is a **content digest**, so the same tag resolves to the same bytecode on
every instance in a fleet. A release is committed once to a replicated store and
every instance converges to it. This lets a fleet run mixed versions during a
rollout without N×M separate deployments, and never serve the wrong version.

## Version selection

Each record carries a version selector in reserved field `255` — the value of the
`X-Module-Version` request header, threaded in at ingress. Per record the pipeline
resolves it against its loaded [version table](../../modules/common/version_core.rs):

- empty selector → the **default** version;
- a tag → the version with that tag;
- anything else → **fail closed**: emit a `{1: "VERSION_UNAVAILABLE"}` record so the
  load balancer retries another instance. An instance never silently runs a version
  it was not asked for.

Selection is **per module** — a request can pin different versions of different
pipelines independently (`X-Module-Version: orders=v2, enrich=green`).

## Hot reload

The version table is mutable at runtime over the module's `control` port (a
`ctrl_input`). Three control ops (`version_core::vctl`):

- `ADD_VERSION` — load a version (replaces a same-tag entry); non-disruptive, records
  in flight keep the version they already resolved;
- `SET_DEFAULT` — repoint the default (the **blue-green flip**);
- `REMOVE_VERSION` — drop a drained version and reclaim its slot.

No restart, no dropped requests. `ADD_VERSION` then `SET_DEFAULT` is a cutover; a
later `SET_DEFAULT` back is a rollback.

## Release manifests

The control-plane unit is a `ReleaseManifest` (`chronicle-canonical::release`): the
set of versions and which tag is the default. It:

- validates representability (`validate()` — bounded count, tag/program lengths, a
  resolvable and unique default) *before* it can be committed, so a bad manifest can
  never produce a param that serves the wrong version;
- lowers to the `versions` param a pipeline loads at startup;
- emits the `ADD`/`SET_DEFAULT` control messages that converge a running instance.

The version-table byte layout has a single writer (`version_core::write_version_entry`),
shared by the host manifest and the device's in-place reload.

## Fleet propagation

A release is committed once to a **replicated key/value store** — Clustor, lattice,
or etcd, all speaking the v3 protocol — under `release/<module>`, at a
monotonically increasing revision:

```
controller ──commit(manifest)──▶  replicated store  ◀──fetch──  every instance
                                        │
                       each instance's Reconciler diffs the snapshot against what
                       it has applied and emits control messages to converge
```

Each instance runs a `Reconciler`: it fetches the latest snapshot, diffs it against
what it has applied, and returns the control messages to converge — add the versions
it lacks, then flip the default — staging and committing an `ActivationBarrier` at
the revision so the flip is ordered by the replicated-log index (version coexistence
and rollback for free).

## Consistency during a rollout

Because a version is a content digest, the same committed manifest yields identical
bytecode on every instance. Mid-rollout, instances may hold different *sets* of
loaded versions, but:

- a pinned version resolves to the same digest everywhere and only runs where loaded;
- an instance behind the current revision fails closed for a not-yet-loaded version
  (the load balancer retries a caught-up instance);
- the default flips through the atomic, converging manifest.

So the fleet is never *inconsistent* — only, briefly, *incomplete*. A request is
never served an unintended version.

## Related Documentation

- [dataplane.md](dataplane.md) — the pipeline module that runs the version table
- [model.md](model.md) — content-digest identity
- [../guides/authoring.md](../guides/authoring.md) — authoring versioned pipelines
