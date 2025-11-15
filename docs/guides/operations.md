# Chronicle Operations Guide

This lean guide replaces the sprawling operational appendices with a single,
current reference for running Chronicle in production. Use it alongside
`docs/specification.md` for the canonical contract, plus `docs/guides/observability.md`
and `docs/guides/security.md` for telemetry and hardening details.

## 1. Topology & Lifecycle
- Run Chronicle as a stateless binary (systemd, Nomad, Kubernetes). Every
  instance loads the same integration YAML and competes for triggers; scale by
  adding replicas, not bespoke configs.
- Front HTTP triggers with your ingress/LB. Chronicle expects plain HTTP on the
  connector’s listen address; terminate external TLS at the edge or see the
  security guide for mTLS notes.
- Kafka chronicles rely on consumer groups. Size `app.limits.kafka.max_inflight_per_partition`
  (plus any `policy.limits.max_inflight` overrides) so each replica only claims
  the concurrency the downstream pool can tolerate; scaling out then becomes a
  linear exercise instead of fighting oversized per-worker buffers.
- Treat warm-up (connectors initialized at boot) as part of the deployment
  lifecycle. Watch readiness (see observability doc), remembering that
  `app.readiness_cache` defaults to 250 ms (minimum 50 ms) and governs both probe
  responses and the HTTP fast-path Chronicle uses before accepting work.
- Route-level queues remain bounded by `app.limits.routes.max_queue_depth`
  (default 1024). Track `route_queue_depth{route}` and `chronicle_shed_total{route}`
  to know when overflow policies start rejecting or shedding requests.

## 2. Runtime Controls
- **Tokio scheduling**: The binary uses `#[tokio::main(flavor = "multi_thread")]`.
  Tune `TOKIO_WORKER_THREADS` to ~`2 x CPU cores` if you routinely process
  thousands of in-flight actions.
- **Backpressure**: Configure `app.limits.http.max_concurrency`,
  `app.limits.routes.max_inflight`, `app.limits.routes.max_queue_depth`
  (default 1024), and `app.limits.kafka.max_inflight_per_partition` to keep
  concurrency bounded. Start with values near your async worker count for HTTP
  (≈256) and ~50 inflight Kafka records per partition, then tune using the
  enforcement counters described below.
- **Connector flags**: Toggle runtimes with `connector_flags.*` (e.g.
  `connector_flags.rabbitmq = false`). Ship flags before enabling new transports
  so configs can land early.
- **Warm-up budgets**: Configure retry/timeout knobs per connector so readiness
  reflects reality. The unified warm-up coordinator marks dependencies as
  `WARMING_UP` and logs duration/attempts; keep budgets under a minute to avoid
  masking failures.

### Backpressure quick reference
- **HTTP triggers**: `app.limits.http.max_concurrency` caps concurrent request
  handling. When the limit is hit, Chronicle responds with `429 + Retry-After`
  (or `503` if the route is already NOT_READY) and increments
  `chronicle_limits_enforced_total{policy="http.max_concurrency"}`. Pair that
  counter with `route_queue_depth{route}` to decide when to raise the limit or
  scale out.
- **Kafka triggers**: `app.limits.kafka.max_inflight_per_partition` (plus any
  `policy.limits.max_inflight`) controls parallel message handling. Exhausting
  the permits pauses polling, toggles `connector_paused{connector}`, and surfaces
  in `chronicle_limits_enforced_total{policy="kafka.max_inflight"}`. Compare
  those signals with broker lag to decide whether to widen the limit or add
  replicas.
- **Overflow semantics**: Routes that set `overflow_policy: queue` rely on the
  1024-default `app.limits.routes.max_queue_depth`; watch
  `route_queue_depth{route}` to catch build-ups early. Routes that `reject` or
  `shed` increment `chronicle_limits_enforced_total` and/or `chronicle_shed_total`,
  which should page the team if they climb faster than `chronicle_actions_total`.
- **Operational heuristics**: scale up when `chronicle_limits_enforced_total`
  and `route_queue_depth{route}` rise together while throughput (`chronicle_actions_total`)
  stays flat; scale down only after those counters remain near zero and queue
  depth rarely exceeds half the configured ceiling. Alert expressions live in
  `docs/guides/observability.md`.

## 3. Connector Handling
- **HTTP clients** reuse a single `reqwest::Client` per connector; set CA and
  optional client cert/key paths relative to the integration file.
- **Kafka producers/consumers** share cached `rdkafka` handles. Populate TLS and
  SASL blocks directly in YAML; Chronicle sets `security.protocol`.
- **Database connectors** (MariaDB/Postgres) share pool builders. Keep pool sizes
  small (≤10) unless latency requires more; Chronicle raises telemetry when pools
  are exhausted.
- **Broker publishers** (RabbitMQ/MQTT/Redis) now route through a shared facade,
  so timeout/backoff decisions are centralized—set defaults once per connector.
- When rolling out new transport code, deploy binaries with the feature flag
  disabled, verify warm-up, then flip the flag and restart.

## 4. Scaling & Resiliency Playbook
- **Observe permit usage**: Watch `route_queue_depth{route}`,
  `chronicle_limits_enforced_total{policy}`, `chronicle_shed_total{route}`, and
  `connector_paused{connector}` to understand when Chronicle is backpressuring.
  Sustained growth means add replicas or widen the relevant `app.limits` knobs.
- **Plan for failure**: Chronicle treats missing jaq inputs as fatal. Use the
  readiness graph to identify which connectors gate each chronicle and pre-stage
  manual overrides for maintenance. Track `chronicle_half_open_probe_concurrency{endpoint}`
  and `chronicle_retry_attempts_total{route,endpoint}` to confirm breakers are
  probing within budget instead of endlessly flapping.
- **Management surface**: `/live` only reports process health; `/ready` and
  `/status` mirror the readiness controller with caching to avoid flapping.
  Refer to `docs/guides/observability.md` for payload schemas before wiring probes.
- **Runbooks**: Keep per-connector recovery steps handy (broker restarts, flag
  rollbacks, replay scripts). Each should reference the canonical chronicle IDs
  and connector names from the integration file.

## 5. Release Workflow
1. Run `make verify` (fmt, clippy `-D warnings`, tests, feature matrix) before
   every PR. Optional heavy checks can be skipped locally via `SKIP_AUDIT=1` or
   `SKIP_SIZE_CHECK=1`, but CI enforces them.
2. Validate integration YAML against `docs/reference/chronicle.schema.json`
   with `make schema-validate CONFIG=path/to/your/config.yaml` (or run the binary
   directly via `cargo run --bin schema_validate -- path/to/config.yaml`). The CI
   pipeline runs the same check against fixtures; keep production configs aligned.
3. Use `make harness-up && make integration-tests` to exercise the Docker
   harness when transport code changes; tear down with `make harness-down`.
4. Audit dependencies/size with `tools/audit-deps.sh` and `tools/size-build.sh`
   before tagging releases. Track the ≤8 MB target and address drift promptly.
5. Document rollout decisions (feature flags, retry/backoff defaults, scaling
   numbers) in your ops runbook or ticketing system instead of growing
   documentation here.

## 6. Production Checklist
- Connector flags set as intended per environment.
- Dashboards wired to new metrics (see observability guide) and alerts assigned
  to on-call rotations.
- Backoff defaults recorded for every runtime touching external brokers/DBs.
- Smoke tests (`cargo test --test message_brokers`, fixture-driven suites) run
  against staging harnesses before promotions.
- Operations/SREs know how to override readiness and how to interpret the
  readiness graph when circuits open.

This guide intentionally omits historical notes and superseded pipelines. If you
need legacy details, refer to the git history or archived tags rather than
adding new appendices.***
