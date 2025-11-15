# Chronicle Technical Specification

## 1. Configuration Overview
Chronicle loads a single YAML document that now carries both configuration and operational policy. Five top-level sections are recognised:

- `api_version`: version tag for schema evolution and feature gating.
- `app`: application-wide policy controlling warm-up, readiness thresholds, limits, and readiness caching.
- `connectors`: reusable connection definitions for external systems.
- `chronicles`: workflow definitions, each describing an atomic execution chain.
- `management`: optional operational endpoints for health and metrics.

Any other top-level key is rejected during validation to catch typos early; the loader surfaces `unknown top-level key "<field>"` and continues collecting additional errors so operators can fix entire batches in one edit.

Chronicle accepts standard YAML anchors/aliases for reuse, but exactly one document per file is permitted. Attempting to load multi-document YAML emits `error[root]: multiple YAML documents are not supported` so operators do not accidentally concatenate staging configs.

**Configuration topology at a glance**
```
connectors --(instantiate)--> endpoints (connector + role)
endpoints --(compose)--> routes (chronicles)
routes --(feed)--> readiness controller & management APIs
```
This identity chain stays consistent across `/status`, metrics labels, and circuit-breaker state so operators can reason about a single name per dependency.

Example structure:
```yaml
api_version: v1
app:
  min_ready_routes: all
  half_open_counts_as_ready: route
  warmup_timeout: 30s
  readiness_cache: 250ms
  limits:
    routes:
      max_inflight: 1024
      overflow_policy: reject
      max_queue_depth: 2048
    http:
      max_concurrency: 512
      max_payload_bytes: 1048576
    kafka:
      max_inflight_per_partition: 32
  retry_budget:
    max_attempts: 5
    max_elapsed: 30s
    base_backoff: 50ms
    max_backoff: 5s
    jitter: full

connectors:
  - name: sample_kafka_cluster
    type: kafka
    options: { brokers: kafka-1:9092 }

chronicles:
  - name: collect_record
    trigger: ...
    phases: ...

management:
  port: 9090
  live: { path: /live }
  ready: { path: /ready }
  metrics: { path: /metrics }
  status: { path: /status }
```

### 1.0 App Field Summary

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `min_ready_routes` | `int` or `all` | `all` | Minimum number of routes that must report `READY` before `/ready` returns success. Numeric values are evaluated in ascending `policy.readiness_priority`. |
| `half_open_counts_as_ready` | `never` \| `endpoint` \| `route` | `route` | Governs whether `CB_HALF_OPEN` endpoints contribute to readiness globally; routes can override via `policy.half_open_counts_as_ready`. |
| `warmup_timeout` | duration | `30s` | Maximum time allotted per endpoint warm-up attempt before it is marked `UNHEALTHY`. |
| `readiness_cache` | duration | `250ms` | Cache window for readiness verdicts; may not be set below `50ms`. |
| `limits.routes.max_inflight` | integer | `1024` | Hard ceiling per route. Must be ≥1 and at least as high as any `policy.limits.max_inflight`. |
| `limits.routes.overflow_policy` | `reject`\|`queue`\|`shed` | `reject` | Controls excess work handling. Selecting `queue` requires `limits.routes.max_queue_depth`. |
| `limits.http.max_concurrency` | integer | `512` | Shared HTTP concurrency semaphore. Route overrides may only lower this value. |
| `limits.http.max_payload_bytes` | integer | `1048576` | Request payload size limit; HTTP triggers reject larger bodies with `413`. |
| `retry_budget.max_attempts` | integer | `5` | Global retry ceiling; intersected with connector/route/phase budgets using the most conservative value. |
| `retry_budget.base_backoff` / `max_backoff` | duration | `50ms` / `5s` | Bounds exponential backoff windows; `max_backoff` must be ≤ `max_elapsed`. |
| `retry_budget.jitter` | `none`\|`equal`\|`full` | `full` | Default jitter applied to any retry plan lacking an explicit jitter selection. |

`docs/reference/chronicle.schema.json` enumerates every nested field and default shown above so schema-aware tooling can validate configs without reverse-engineering this document.

> **Normative keywords:** Statements that use `MUST`, `MUST NOT`, `SHOULD`, or `MAY` follow RFC 2119 meaning and map directly to schema/loader guarantees. Unless a rule explicitly says otherwise, violating a `MUST`/`MUST NOT` condition results in a validation error rather than a runtime fallback.

### 1.1 Core Runtime Concepts
- **Connector (Consumer/Listener):** Declarative, reusable configuration objects that describe how to reach an external system. A connector may operate as an inbound listener (`role = server` or trigger-specific consumer settings) or as an outbound client (`role = client`). Connectors exist exactly once per configuration, regardless of how many routes reference them.
- **Endpoint (Producer/Target):** A concrete runtime instance derived from a connector when it is used by an outbound phase. Endpoint identity is the tuple `{connector_name, role}`; breakers, warm-up state, metrics labels, and `/status.endpoints[]` entries are keyed by that tuple. Multiple routes referencing the same connector therefore share breaker state and telemetry. To isolate an external dependency per route, declare distinct connectors even if their options are identical.
- **Dependency instance:** When a connector is referenced by more than one phase/route, the readiness controller records that shared endpoint once and tracks which routes depend on it. Breaker transitions fan out to every route in the dependency set, while phase-level telemetry continues to report the route/phase origin.
- **Transformation Pipeline:** Chronicle phases form a configurable sequence of pure data-mapping functions that take the inbound slot (`.[0]`) and yield derived JSON to feed downstream phases or responses.
- **Route:** Binding of one Connector to one or more Endpoints via a chronicle's ordered phases plus its delivery policy. Each chronicle compiles into a deterministic route that the readiness controller monitors.
- **Dependency Set:** Minimal set of Endpoints that must be healthy for a Route to accept or process new work. Dependencies are discovered from outbound phases (e.g., HTTP client, Kafka producer, DB write).
- **Health Dimensions:**
  - **Liveness:** Process is running; crash/loop detection only.
  - **Readiness:** Ability to accept or ingest traffic now without immediate failure, combining connector-specific checks with route dependency health.
  - **Target Health:** Endpoint-level connectivity and circuit breaker state.
- **Warm-Up:** Pre-initialisation phase that ensures connectors, pools, and client instances are established before the application can become READY. Warm-up eliminates lazy initialisation and first-request latency, and it is enforced per Endpoint and Route before readiness checks observe success.
- **Workload Limits:** Application- and route-level ceilings on concurrency, queue depth, and payload size that protect memory and provide explicit backpressure when dependencies stall.
- **Retry Budget:** Aggregate envelope of attempts, elapsed time, and backoff that reconciles delivery policies with circuit breaker health to avoid thundering-herd retries during recovery.

Endpoint IDs shown in `/status.endpoints[]`, metrics (`endpoint=` labels), and logs follow `endpoint.<connector_name>` truncated to 36 characters with a deterministic four-character hash suffix whenever truncation occurs (e.g., `endpoint.db-main` or `endpoint.my-long-name-a1b2`). The same truncation algorithm is used everywhere so operators can reliably join `/status` payloads with metric streams without guessing which string normalization was applied, and the truncated strings remain stable across restarts as long as the connector name does not change.

Connector usage determines the endpoint "role" suffix in the identity tuple `{connector_name, role}`:

| Connector family | Role derivation | Notes |
| --- | --- | --- |
| HTTP / gRPC | Explicit `role` field (`server`, `client`) | Validation rejects connectors that omit the field. |
| Kafka / RabbitMQ / MQTT / Redis stream | `producer` when referenced by outbound phases; `consumer` when referenced by triggers | The loader refuses to use the same connector as both producer and consumer simultaneously. |
| Databases (Postgres, MariaDB, MongoDB) / SMTP / HTTP clients | Always `client` | Declaring two connectors with different delivery policies (e.g., idempotent vs. non-idempotent) is required if the runtime settings differ. |

Because roles are inferred from usage for some families, Chronicle validates that a single connector is not referenced by mutually incompatible delivery policies. For example, a Kafka connector cannot be shared between an idempotent route (which forces `acks=all`) and a best-effort route; the validator fails the configuration (before runtime) and instructs the operator to declare two connectors. The same compile-time check prevents a connector from acting as both producer and consumer simultaneously.

### 1.2 Observability & metadata

Chronicle treats trace, record, and route identifiers as first-class metadata. Each trigger preserves inbound `trace_id`/`record_id` values inside `DomainMessage.metadata`, and the execution context exposes them via `ExecutionContext.observability()` so phases, dispatchers, and readiness checks read from the same canonical slot. Logs, management events, and `/metrics` payloads embed these keys to make traces and connector outcomes correlate without forcing operators to reverse-engineer context selectors.

Telemetry counters, histograms, and backpressure snapshots come from the `chronicle::metrics` facade. This helper wraps `telemetry::RuntimeCounters`, keeps the Prometheus families (`chronicle_backpressure_http_*`, `chronicle_backpressure_kafka_*`, `chronicle_connector_actions_total`, `chronicle_http_request_duration_seconds`, etc.) in sync across transports, the management surface, and regression tests, and ensures connectors report the same labels even when new fields are added to the schema described in `docs/guides/observability.md`.

### 1.3 Illustrative Configuration
```yaml
api_version: v1
app:
  min_ready_routes: all
  half_open_counts_as_ready: route
  warmup_timeout: 30s
  readiness_cache: 250ms
  limits:
    routes:
      max_inflight: 1024
      overflow_policy: reject
      max_queue_depth: 1024
    http:
      max_concurrency: 512
      max_payload_bytes: 1048576
    kafka:
      max_inflight_per_partition: 32
  retry_budget:
    max_attempts: 5
    max_elapsed: 30s
    base_backoff: 50ms
    max_backoff: 5s
    jitter: full

connectors:
  - name: http-in
    type: http
    options:
      role: server
      listen_addr: ":8080"
  - name: kafka-in
    type: kafka
    options:
      brokers: ["kafka:9092"]
  - name: kafka-out
    type: kafka
    warmup: true
    warmup_timeout: 10s
    cb: { failure_rate: 0.5, window: "30s", open_base: "1s", open_max: "60s" }
    options:
      brokers: ["kafka:9092"]
  - name: db-main
    type: postgres
    warmup: true
    warmup_timeout: 30s
    cb: { failure_rate: 0.25, window: "30s", open_base: "500ms", open_max: "30s" }
    options:
      url: "postgres://…"
      pool: { min_connections: 3, max_connections: 20 }

chronicles:
  - name: r-http-to-kafka
    trigger:
      connector: http-in
      options:
        method: POST
        path: /events
        content_type: application/json
    phases:
      - name: to_avro
        type: transform
        options:
          record: .[0].body.record
          trace:
            id: .[0].headers.trace_id
            received_at: .[0].metadata.received_at
      - name: publish
        type: kafka
        timeout: 2s
        options:
          connector: kafka-out
          topic: outbound.events
          key: .[0].body.record.id
          body: .[1]
    policy:
      requires: ["kafka-out"]
      delivery: { retries: 3, backoff: "50ms..2s", idempotent: true }

  - name: r-kafka-to-db
    trigger:
      connector: kafka-in
      options:
        topic: inbound.events
        group_id: svc.group
    phases:
      - name: to_sql
        type: transform
        options:
          record_id: .[0].value.id
          attributes: .[0].value.attributes
          metrics: .[0].value.metrics
      - name: persist
        type: postgres
        timeout: 2s
        options:
          connector: db-main
          sql: |
            INSERT INTO records (record_id, attributes, metrics)
            VALUES (:record_id, :attributes::jsonb, :metrics::jsonb)
          values:
            record_id: .[1].record_id
            attributes: .[1].attributes
            metrics: .[1].metrics
    policy:
      requires: ["db-main"]
      delivery: { retries: 5, backoff: "100ms..5s", tx: true }

management:
  port: 9090
  live: { path: /live }
  ready: { path: /ready }
  metrics: { path: /metrics }
  status: { path: /status }
```
This sample demonstrates how warm-up, circuit breakers, and route policy combine with chronicles to drive readiness.

### 1.3 Schema Versioning and Compatibility
- `api_version` is required and currently accepts `v1`. Configuration loading fails if the file omits the field or advertises an unsupported version.
- Version bumps are monotonic and communicate breaking or additive changes. Operators should stage upgrades by validating configs against the new schema before rolling out binaries.
- Feature gates are exposed through `app.feature_flags` to permit opt-in previews while keeping the base schema stable. Unknown gates trigger validation errors. Chronicle currently recognises `rabbitmq`, `mqtt`, `mongodb`, `redis`, `parallel_phase`, and `serialize_phase`; the first four enable their corresponding connector families while the latter two guard the experimental phase executors. Validation fails when a gated connector or phase is configured without the matching feature flag.

#### Feature Flag Matrix (`app.feature_flags`)

| Flag | Enables | Default | Notes |
| --- | --- | --- | --- |
| `rabbitmq` | `type: rabbitmq` connectors, triggers, and phases | `false` | Required before any AMQP resources are parsed or instantiated. |
| `mqtt` | MQTT connectors/triggers/phases | `false` | Guards the MQTT client runtime while it remains optional. |
| `mongodb` | MongoDB connectors and change-stream triggers | `false` | Also gates MongoDB CRUD phases. |
| `redis` | Redis connectors, stream triggers, and phases | `false` | Keeps Redis and Redis Streams disabled until explicitly requested. |
| `parallel_phase` | `type: parallel` phases | `false` | Disallows fan-out definitions entirely when absent. |
| `serialize_phase` | `type: serialize` phases / codec registry | `false` | Without the flag, codec envelopes may still be consumed, but serialize phases fail to load. |

#### Feature Flag List

- Run `chronicle --list-feature-flags` to print the exact `app.feature_flags` names baked into the current binary. The command sources the same list used by validation so operators can script gate discovery without scraping this document.

- Deprecations follow a two-release policy: fields are first marked with `deprecated: true` metadata in the schema, then removed in a subsequent major version. Validation surfaces actionable messages for any deprecated usage.
- A machine-readable JSON Schema (`docs/reference/chronicle.schema.json`) mirrors this specification. It follows draft 2020-12, encodes the enumerations and conditional requirements (e.g., `role = server` → `listen_addr`), and serves as the source of truth for editor integration and CI validation pipelines.
- Run `chronicle validate path/to/config.yaml` during development or CI to invoke the exact same schema (including role-inference rules, limit inequalities, and idempotent-delivery checks) without starting the binary. The CLI emits identical diagnostics to the runtime loader, which keeps automation and local edits aligned.
- Example diagnostics showing aggregated failures:
  ```console
  $ chronicle validate config/bad.yaml
  error[app.limits.routes.max_inflight]: must be ≥ 1 (got 0)
  error[root]: unknown top-level key "apps" (expected one of api_version, app, connectors, chronicles, management)
  error[chronicles[0].policy.limits.max_inflight]: 4096 exceeds app.limits.routes.max_inflight (1024)
  ```
  The loader collects as many errors as possible in a single pass (failing only on unrecoverable parsing issues) so CI surfaces whole batches of violations instead of one at a time.
- Every diagnostic payload also includes `schema_version: "v1"` (or the matching semantic version) so CI can ensure the CLI and runtime agree on validation semantics.
- Both `chronicle validate` and the runtime loader resolve `docs/reference/chronicle.schema.json` relative to their current working directory; released binaries embed an identical copy and fall back to that bundled schema when the relative path is unavailable. Scripts that invoke the CLI from another directory should pass `--schema /absolute/path/to/docs/reference/chronicle.schema.json` explicitly.
- The JSON Schema also encodes cross-scope comparisons (`policy.limits ≤ app.limits`, `max_backoff ≤ max_elapsed`, connector role separation, etc.) and is exercised by regression fixtures under `tests/fixtures/*`. Add a fixture whenever you introduce a new validation rule so CI catches drift early.
- `api_version: v1.1` is reserved for the next wave of additive features (streaming/serialization improvements). New behaviour should be hidden behind feature flags until the version bump is declared so upgrades remain predictable.

### 1.4 Limits and Flow Control
- `app.limits.routes.max_inflight` caps concurrent executions per route. Work beyond the threshold follows `overflow_policy` (`reject`, `queue`, or `shed`). `queue` uses an in-memory bounded queue governed by `max_queue_depth`. Operators are expected to set the field explicitly (the recommended starting value is `1024`). Selecting `overflow_policy: queue` without providing `max_queue_depth` is a validation error that reminds operators of the recommended starting point: `limits.routes.max_queue_depth must be set (default 1024) when overflow_policy=queue`.
- **Overflow semantics**
  - `reject`: surplus work is denied immediately. HTTP triggers respond with `429 Too Many Requests` (and `Retry-After`) when the rejection is purely due to concurrency/queue pressure, or `503 Service Unavailable` when readiness is already `NOT_READY`. Async triggers (Kafka, RabbitMQ, MQTT) do not acknowledge or commit the event; it remains in the source system for later re-processing.
- `queue`: work enters the bounded queue until `max_queue_depth` is reached. Hitting the depth limit surfaces the same outcomes as `reject`. The queue depth is exported via `chronicle_route_queue_depth{route}` so SLOs can track saturation.
  - `shed`: Chronicle refuses the excess unit of work before ingestion. HTTP connectors return `503 Service Unavailable` with `Retry-After` derived from the retry budget. Kafka (and other pull-based triggers) pause polling to keep offsets uncommitted; no message is dropped at the broker. Shed counters increment `chronicle_shed_total{route}` separate from `chronicle_limits_enforced_total`.
- Connector-specific limits enforce backpressure close to ingress: `app.limits.http.max_concurrency` bounds simultaneous HTTP requests and integrates with readiness. Requests above the limit receive `429 Too Many Requests` with `Retry-After` derived from the retry budget when the route remains healthy.
- Limit-based `429` responses use a consistent JSON payload: `{"error":"LIMITS_ENFORCED","route":"<id>","retry_after_seconds":<int>,"ts":"<rfc3339>"}`. The `retry_after_seconds` field reflects the same budget-derived delay exposed in the `Retry-After` header, and Chronicle caches the body in the same way it caches readiness verdicts so that aggressive clients see deterministic backpressure signalling within the cache window.
- `app.limits.http.max_payload_bytes` rejects oversized request bodies before parsing, protecting slot memory pressure. Similar guards exist for Kafka (`max_inflight_per_partition`) and the other managed connectors (RabbitMQ, MQTT, Redis, MongoDB, Postgres), each surfacing metrics on the enforcement path.
- Limits are also available per chronicle via `policy.limits`. Route limits override application defaults, enabling high-priority flows to reserve capacity while others use shared ceilings.
- All numeric limits must be greater than zero. Selecting `overflow_policy: queue` (globally or per-route) requires `max_queue_depth` so the in-memory buffer remains bounded.
- **Limit precedence:** Connector-specific ceilings (e.g., `kafka.max_inflight_per_partition`, DB pool sizes) are enforced first, followed by route-level overrides, then application-wide defaults. Route limits may only lower the effective ceiling; they cannot raise a connector/app limit. Validation enforces `policy.limits.max_inflight ≤ app.limits.routes.max_inflight` and similar per-transport rules (`policy.limits.http.max_concurrency` cannot exceed `app.limits.http.max_concurrency`, etc.). If operators need a higher ceiling for a subset of routes, they must raise the app-level default (or declare a dedicated connector) first.

### 1.5 Retry Budgets
- `app.retry_budget` defines the global retry envelope applied when routes or connectors omit explicit overrides. Budgets combine attempt counts, elapsed time ceilings, exponential backoff, and jitter (`none`, `equal`, or `full`).
- Routes may override budgets inside `policy.retry_budget`. Connector-level budgets can further tighten retries for specific external systems; when provided, the effective policy is the intersection of application, route, and connector budgets.
- Circuit breakers honour budgets by failing fast when the remaining elapsed budget cannot accommodate another attempt. Budget exhaustion transitions the route to `NOT_READY` and increments breaker failure metrics.
- Budgets emit `chronicle_retry_budget_exhausted_total` metrics labelled by route and endpoint to highlight overload or persistent dependency failure.
- `delivery.retries` (either on a phase or inherited from route `policy.delivery`) is an intent, but an attempt only proceeds when the effective retry budget still grants capacity. The runtime evaluates `attempt_permitted = (attempts + 1) <= max_attempts ∧ (elapsed + next_backoff) <= max_elapsed`. Example: if a phase asks for 10 retries but the effective budget allows only 3 attempts within 5 s, Chronicle stops at 3 attempts even if `delivery.retries` is higher and logs `BUDGET_EXHAUSTED`. Validation surfaces a diagnostic whenever `policy.delivery.retries` (or a phase override) exceeds the tightest `retry_budget.max_attempts`, making the truncation explicit during `chronicle validate`.
- Backoff windows are taken from `delivery.backoff` when provided; otherwise they inherit from the effective retry budget (`app.retry_budget` intersected with connector and route overrides). The `delivery.backoff` syntax accepts either a single duration (`250ms`) for fixed spacing or a `min..max` range (`50ms..2s`), which Chronicle interprets as exponential backoff starting at `min`, doubling every attempt, and capping at `max`. Regardless of the source, jitter settings come from the effective budget (`jitter` defaults to `none` unless specified) and are applied after calculating the deterministic window.
- Think of `policy.retry_budget` as the envelope (attempt count + elapsed ceiling + clamps) and `policy.delivery.backoff` as the shape of each retry plan. Delivery settings never override the envelope; if a `min..max` schedule would exceed `max_elapsed` or require more attempts than allowed, the runtime truncates the plan and raises `BUDGET_EXHAUSTED` instead of silently stretching the budget.
- Validation rejects retry budgets where `max_backoff` exceeds `max_elapsed`; when the two are equal Chronicle warns operators to keep `max_elapsed` slightly higher so the final attempt has room to succeed.
- Configurations where `max_attempts = 1` but `max_elapsed = 0s` (or any non-positive duration) are rejected because they would prohibit even the initial attempt. All elapsed windows MUST be strictly greater than zero whenever attempts are permitted.
- Jitter is applied after clamping to `max_backoff`, and Chronicle refuses settings where jitter would push `(elapsed + backoff)` beyond `max_elapsed`; requests receive `BUDGET_EXHAUSTED` without issuing the attempt rather than violating the ceiling.
- **Field-wise intersection** — Whenever multiple retry budgets apply (application, connector, route/policy, phase), Chronicle resolves each field independently using the most conservative value:

| Field | Resolution rule |
| --- | --- |
| `max_attempts` | Minimum across all scopes |
| `max_elapsed` | Minimum across all scopes |
| `base_backoff` | Maximum across scopes (slowest ramp-up wins) |
| `max_backoff` | Minimum across scopes |
| `jitter` | Route-level jitter overrides connector/app defaults; when multiple scopes specify jitter, the runtime selects the highest randomness according to `none < equal < full`. |

For example, if the app budget allows `max_attempts=5`, a connector tightens it to `3`, and a route requests `10`, the effective ceiling is `3`. Likewise, a connector with `base_backoff=500ms` and a route with `base_backoff=100ms` yields `500ms`, ensuring the slowest start governs recovery.

Unless a rule explicitly calls out a warning (e.g., `max_backoff = max_elapsed` emits a reminder to keep `max_elapsed` slightly higher), equal values across scopes pass silently. Only values that exceed the safety bounds (`<= 0`, `max_backoff > max_elapsed`, route limits higher than app limits, etc.) fail validation.

The implementation follows a simple intersection reducer that select the most conservative outcome per field:

```rust
fn effective_retry_budget(scopes: &[RetryBudget]) -> RetryBudget {
    scopes.iter().skip(1).fold(scopes[0].clone(), |mut acc, budget| {
        acc.max_attempts = acc.max_attempts.min(budget.max_attempts);
        acc.max_elapsed = acc.max_elapsed.min(budget.max_elapsed);
        acc.base_backoff = acc.base_backoff.max(budget.base_backoff);
        acc.max_backoff = acc.max_backoff.min(budget.max_backoff);
        acc.jitter = acc.jitter.most_conservative(budget.jitter);
        acc
    })
}
```
`most_conservative` resolves to `none < equal < full`, mirroring the precedence rules above: the runtime deliberately applies the jitter configuration that introduces the most randomness to avoid synchronized retries when any scope opts in to additional jitter.
This helper assumes `RetryBudget: Clone`, which matches the concrete implementation in the runtime.

### 1.6 Half-Open Readiness Policy
- `app.half_open_counts_as_ready` controls whether endpoints or routes in `CB_HALF_OPEN` state count toward readiness gates. Accepted values are `never`, `endpoint`, and `route`. `route` (the default) allows half-open endpoints to contribute to route readiness but still logs a degraded warning; `never` enforces a stricter posture.
- Overrides can be set per route via `policy.half_open_counts_as_ready`, enabling critical flows to demand proven recovery while less critical paths resume sooner.
- The controller permits at most two concurrent probes per endpoint while it is half-open. This cap is a compile-time constant (currently fixed at two probes with no configuration knob) so operators can reason about probe load. Each probe consumes from the same retry budget that governs steady-state delivery, so repeated probe failures both delay recovery and emit `chronicle_half_open_probe_concurrency{endpoint}` and `chronicle_retry_budget_exhausted_total` metrics. When the budget cannot cover another probe (attempt count or elapsed ceiling), the breaker remains `CB_OPEN` until the global budget is refreshed.
- The retry envelope is shared: when the global/app-level budget refreshes, both steady-state delivery attempts and half-open probes regain eligibility at the same time; there is no separate half-open pool. This keeps breaker healing and production traffic aligned while preventing hidden extra retries.

### 1.7 Duration and Clock Semantics
- All durations use Go-style strings (`1s`, `150ms`, `2m30s`). Numeric millisecond suffixes are deprecated and rejected in `api_version: v1`. Warm-up, drain, and retry/backoff durations must be greater than zero; zero-valued windows are rejected during validation.
- `Retry-After` headers and management timestamps derive from the monotonic-clock-backed system time. Chronicle tolerates ±2 s skew when comparing client-provided timestamps; larger differences emit warnings.
- Warm-up, retry, and readiness timers rely on monotonic sources where available to prevent retrograde time jumps from unblocking gates prematurely.
- `app.readiness_cache` defaults to `250ms` and may not be set below `50ms` to prevent controller thrash; connectors rely on this cache for per-request readiness checks. `ready` responses are cached for the entire window, so probes that hammer `/ready` faster than the cache interval receive the same status, headers, and body that were computed at the start of the window. When the state flips inside the window, the change becomes visible immediately after the cache expires, and HTTP connectors reuse the cached verdict to fast-path decisions.
- Readiness-driven `503` responses always set `Retry-After` to the remaining cache window (rounded up to one whole second). Limit-driven `429` responses set `Retry-After` to the next backoff derived from the effective retry budget, clamped so `elapsed + next_backoff ≤ max_elapsed`.
- Duration guardrails at a glance:

| Field / Setting | Constraint | Rationale |
| --- | --- | --- |
| Warm-up, drain, retry, and timeout durations | `> 0` | Prevents zero-delay busy loops and ensures watchdogs eventually fire. |
| `app.readiness_cache` | `≥ 50ms` | Keeps readiness cache churn bounded for HTTP/Kafka controllers. |
| `retry_budget.max_backoff` | `≤ retry_budget.max_elapsed` | Guarantees the final attempt has time to complete. |
| `app.drain_timeout` | `> 0` (default `30s`) | Ensures shutdown either completes gracefully or times out deterministically. |
| Connector/phase `timeout` overrides | `> 0` | Keeps breaker accounting meaningful and prevents hung I/O. |

- Signed payload validation (JWTs, HMACs, expiry claims) is extremely sensitive to skew beyond the ±2 s tolerance. Chronicle does not adjust claims automatically; operators should either enforce validation upstream, ensure NTP-synchronised clocks in every deployment, or provision a pre-validation phase that rejects requests when `now` drifts outside the acceptable skew.
- **Time references at a glance**

| Purpose | Tolerance / Window |
| --- | --- |
| Client-provided timestamps (Retry-After, jaq comparisons) | ±2 s |
| TLS expiry watchdog (`chronicle.tls.expiry_seconds`) | ±5 s clock agreement required to avoid false alarms |
| Readiness cache (`app.readiness_cache`) | ≥50 ms, default 250 ms |
- These tolerances are currently fixed; if your environment requires looser settings, adjust host-level NTP rather than Chronicle's configuration.

### 1.8 Lifecycle Controls
- `app.drain_timeout` (default `30s`) bounds graceful shutdown. The controller enforces a hard stop five seconds after the deadline to avoid hung drains.
- Chronicle currently requires a process restart to pick up new configuration. Hot reload endpoints are not available in this release.

## 2. Connectors

### 2.0 Connector Field Summary

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `name` | string | — | Unique identifier. Appears in `/status`, metrics, and dependency sets; renaming changes endpoint labels. |
| `type` | enum | — | One of the supported transport families (`http`, `kafka`, `postgres`, etc.). Determines which `options.*` keys are legal. |
| `options` | map | — | Transport-specific settings (brokers, DSNs, credentials). Nested structure is validated against the connector type. |
| `warmup` | bool | `false` | Forces eager connection establishment; required when downstream dependencies must be ready before ingress. |
| `warmup_timeout` | duration | `30s` | Per-attempt bound for warm-up tasks. |
| `cb.*` | object | — | Circuit breaker thresholds (`failure_rate`, `window`, `open_base`, `open_max`). All fields are required when the block is present. |
| `timeouts.*` | object | — | `connect`, `request`, `read`, `write`, `query` keys map to transport deadlines. |
| `max_payload_bytes` | int | transport default | Applies to producer-style connectors to guard against oversized messages before the broker rejects them. |
| `requires` | array | inferred dependencies | Explicit dependency list when a connector fans out to multiple logical endpoints. |
| `retry_budget.*` | object | inherits from `app.retry_budget` | Narrows retries for a specific dependency; intersected with route/app scopes. |
| `limits.*` | object | transport-specific | Allows connectors (e.g., Kafka, Redis) to cap concurrency before route/app ceilings. The schema enforces that routes cannot exceed these values. |
| `feature_flag` | string | — | Some connectors (RabbitMQ, MQTT, MongoDB, Redis, parallel/serialize-phase helpers) require an enabling flag under `app.feature_flags`. |

### 2.1 Common Schema
Each connector is defined as:
```yaml
- name: <string>
  type: <string>
  options: <map>
```

| Field | Type | Required | Description |
|-------|------|-----------|-------------|
| `name` | string | yes | Unique identifier used to reference the connector. |
| `type` | string | yes | One of `http`, `kafka`, `mariadb`, `postgres`, `rabbitmq`, `mqtt`, `smtp`, `mongodb`, `redis`, `grpc`. |
| `options` | map | yes | Typed configuration object. Nested structures are expressed with native YAML. |

Additional cross-cutting options are shared across connector types when used as outbound endpoints:
- `warmup`: boolean (default `false`). When `true`, the connector must complete warm-up before the application exits `WARMING_UP`.
- `warmup_timeout`: duration string (default `30s`). Bounds each warm-up attempt before the endpoint is marked `UNHEALTHY`.
- `cb`: optional circuit breaker configuration (`failure_rate`, `window`, `open_base`, `open_max`) that governs retries and open/half-open behaviour.
- `timeouts`: optional map defining per-request or per-operation bounds. Chronicle recognises `connect`, `request`, `read`, `write`, and `query`. HTTP and gRPC connectors currently apply the `connect`/`request` values directly to their client builders, while the remaining keys are available to DB/queue connectors that expose query-oriented operations.
- `max_payload_bytes`: optional integer used by producer-style connectors (Kafka, RabbitMQ, MQTT, Redis, SMTP, gRPC) to reject payloads before they hit broker-imposed limits. When omitted, Chronicle defers entirely to the transport's defaults; operators are encouraged to set the field to a safe value that matches downstream quotas so validation errors surface early.
- `requires`: optional explicit list of downstream dependency identifiers when a single connector fans out to multiple logical endpoints; omit to let the route infer dependencies from phase usage. Whenever a connector lists `requires`, every Route that references the connector automatically picks up the additional dependencies so readiness reflects the transitive health checks.

These options allow the readiness controller to compute Dependency Sets and to enforce warm-up, circuit breaking, and backpressure semantics described later in this document.
Unless otherwise noted, cross-cutting options are declared alongside `name`, `type`, and `options` at the connector root.
Chronicle rejects configurations that misplace these keys under `options.*`; the loader surfaces `connector.<name>.<field> must be declared at the connector root` to keep schemas predictable and tooling (like the JSON Schema) effective.
Connector-level `requires` therefore act globally for that connector, while `policy.requires` (Section 3.3) only amends the dependency set for the single route that declares it.
If TLS/auth parameters are provided both inside a DSN/URL and at the connector root, the root-level values win. Chronicle emits a warning and coerces the DSN flags (`sslmode`, `?tls=` etc.) to match the authoritative connector fields so runtime behaviour cannot diverge silently.

Throughout this specification, examples share a generic nested payload theme:
- `record.id` — identifier for the item being processed.
- `record.attributes.*` — nested descriptive attributes (e.g., `category`, `tier`).
- `record.metrics.*` — nested numeric metrics such as `latency_ms`.
- `record.observed_at` — upstream capture timestamp.
This consistent, domain-agnostic shape keeps the samples cohesive while demonstrating nested data handling.

#### Transport and Authentication Model
- **Transport** — TLS is disabled when the `tls` object (or a TLS-preferring URI scheme) is absent. Provide a `tls` map containing at least the CA (`ca`) and, when mutual TLS is required, the connector's `cert` and `key`. Client connectors may also rely on TLS-specific URI schemes such as `https`, `amqps`, `mqtts`, `rediss`, or `mongodb+srv`.
- TLS materials must be PEM-encoded; passphrase-protected keys are not supported. Unless a connector explicitly supplies `tls.ca`, Chronicle falls back to the platform's system roots. Setting `tls.mode: verify_full` (or `tls.domain`) tells Chronicle to perform hostname validation; validation fails when strict verification is requested without a `tls.domain` override or a resolvable hostname in the URL.
- **Authentication** — Username/password credentials are supplied either via explicit `username`/`password` fields (e.g., HTTP, MQTT, Redis) or inside connection URLs/DSNs (e.g., MariaDB, Postgres). Client-certificate authentication uses the same `tls.cert`/`tls.key` pair (sometimes exposed as `client_cert`/`client_key`) and therefore automatically requires TLS transport.
- When the `cb` block is provided, all four fields are required. `failure_rate` must be greater than `0.0` and at most `1.0`, every duration (`window`, `open_base`, `open_max`) must be greater than zero, and `open_max` must be greater than or equal to `open_base`.

#### Secrets and TLS Assets
- Connector TLS file paths are resolved relative to the integration file by default; absolute paths remain supported for operators that mount certificates elsewhere. When replacing TLS material and restarting Chronicle, the previous material stays active until the replacement parses successfully, and a failed restart clearly reports `connector.<name>.tls.*` in `/status`.
- Chronicle deliberately avoids reading secrets from shell environment variables unless the configuration uses `${ENV_VAR}` interpolation; when interpolation is used, the resolved value is redacted in logs/metrics and only the placeholder remains visible. Prefer file-backed secrets (KMS-decrypted bundles, tmpfs mounts, or Kubernetes secrets) so restarts can rotate credentials without rebuilding the binary.
- Logs, `/status`, and metrics mask usernames, passwords, API tokens, client keys, and inline TLS material with `"<redacted>"`. File paths remain visible so operators can audit rotations.
- For security reasons, `tls.key` and other private-key fields MUST reference filesystem paths. Chronicle rejects inline PEM blobs (those beginning with `-----BEGIN PRIVATE KEY-----`) so operators do not accidentally bake credentials into the config blob or version control.

### 2.2 Connector Types

#### **HTTP Connector**
Options:
- `role`: required, `server` or `client`.
- `listen_addr`: required when `role = server`.
- `base_url`: required when `role = client`.
- `tls`: optional nested object with `key`, `cert`, `ca`.
- `health`: optional nested object (`method`, `path`) describing an HTTP health probe for servers.
- `timeouts` and other advanced options may be included as additional nested keys.
- `default_headers` are applied to every HTTP client request before per-phase headers, making it easy to stamp trace metadata on all calls made through the connector.
- **Transport combinations**
  - *Plaintext*: omit the `tls` block to expose HTTP listeners or invoke `http://` targets.
  - *TLS*: supply `tls.cert`, `tls.key`, and optionally `tls.ca` to terminate or initiate HTTPS connections.
- **Timeout integrations**
  - When `timeouts.connect` is supplied, Chronicle applies it to the underlying `reqwest` client's connect timeout. `timeouts.request` becomes the default per-request deadline for phases that do not specify their own `timeout`.
- **Authentication combinations**
  - *Username/password*: use `auth.username`/`auth.password` (for HTTP Basic) or embed credentials inside `base_url`.
  - *Client certificate*: supply the TLS cert/key pair to enable mutual TLS, which implicitly enforces TLS transport.
- **Readiness integration**
  - HTTP connectors operating as servers consult the readiness cache (bounded by `app.readiness_cache`) before reading the request body or dispatching work.
  - When the associated Route is `WARMING_UP` or `NOT_READY`, Chronicle now returns `503 Service Unavailable` with `Retry-After: <seconds>` derived from the cache window, sets `Cache-Control: no-store`, and emits a JSON payload `{"error":"TARGET_UNAVAILABLE","route":"<id>","ts":"<rfc3339>"}`. The decision happens before request bodies are parsed, so stalled dependencies never consume application capacity.
  - Payloads larger than `app.limits.http.max_payload_bytes` are rejected with `413 Payload Too Large`, and the listener acquires a global HTTP backpressure permit before executing a Route so `app.limits.http.max_concurrency` remains effective even when clients retry aggressively.

#### **Kafka Connector**
- Keys:
  - `brokers`: comma-separated list of broker addresses.
  - `create_topics_if_missing`: optional boolean (default `false`) that requests Kafka to automatically create a topic the first time a producer targets it. Chronicle sets `allow.auto.create.topics` on the underlying client when enabled.
  - `security`: optional nested object with SASL/SSL settings (e.g., `security.sasl.mechanism`, `security.tls.ca`).
- Used for both producer and consumer operations.
- `create_topics_if_missing` should remain disabled in production clusters that enforce ACLs; Chronicle logs a warning if brokers reject auto-create so operators can provision topics explicitly.
- **Transport combinations**
  - *Plaintext*: no `security.tls` block; brokers are contacted over TCP.
  - *TLS*: populate `security.tls` with `enabled`, `ca`, and optional `client_cert`/`client_key` to secure traffic.
- **Authentication combinations**
  - *Username/password*: configure `security.sasl.username` and `security.sasl.password`.
  - *Client certificate*: configure `security.tls.client_cert` and `security.tls.client_key` (TLS must be enabled).
- **Connector control**
  - Kafka consumers integrate with the readiness controller to pause partitions when the Route is `WARMING_UP` or `NOT_READY`, resuming automatically once dependencies recover.
  - Max in-flight processing limits prevent additional polls while paused; commit offsets only after successful Endpoint writes.
- Producers should enable idempotent mode when `delivery.idempotent=true` to honour at-least-once semantics without duplicates. Chronicle enforces `enable.idempotence=true`, `acks=all`, and `max.in.flight.requests.per.connection <= 5` whenever an idempotent delivery policy touches a Kafka connector.

#### **MariaDB Connector**
- Keys:
  - `url`: required, full DSN.
  - `schema`: optional logical schema.
  - `tls`: optional object with `mode`, `ca`, `cert`, and `key`.
    - `mode` accepts `disable`, `prefer`, `require`, `verify_ca`, or `verify_full` and defaults to `verify_ca`; `verify_full` maps to MySQL's `VERIFY_IDENTITY` check.
  - `pool`: optional object with `min_connections`, `max_connections`, and timeout tuning; used by warm-up enforcement.
- Provides idempotent insert or custom SQL functionality.
- **Transport combinations**
  - *Plaintext*: DSN uses `mysql://` (or omits TLS query parameters) and no `tls` block is present.
  - *TLS*: add the `tls` object and/or `?tls=true` in the DSN to negotiate TLS.
- **Authentication combinations**
  - *Username/password*: embed credentials inside the DSN (e.g., `mysql://user:pass@host/db`).
  - *Client certificate*: provide `tls.cert` and `tls.key`; doing so requires TLS to be enabled.
- **Warm-up expectations**
  - When `warmup: true`, Chronicle opens connections until `pool.min_connections` is satisfied during warm-up. Failure to reach the threshold within `warmup_timeout` marks the endpoint `UNHEALTHY` and prevents readiness.

#### **Postgres Connector**
- Keys:
  - `url`: required, Postgres connection string.
  - `schema`: optional default schema.
  - `tls`: optional object with `mode`, `ca`, `cert`, and `key`.
    - `mode` accepts `disable`, `prefer`, `require`, `verify_ca`, or `verify_full` and defaults to `verify_ca`; `verify_full` enforces hostname validation in addition to CA checks.
  - `pool`: optional object with `min_connections`, `max_connections`, `connect_timeout`, etc.; required when warm-up checks enforce connection thresholds.
- Provides the same SQL execution capabilities as MariaDB connectors but targets PostgreSQL wire protocol.
- **Transport combinations**
  - *Plaintext*: DSN uses `postgres://` without TLS parameters and the `tls` block is omitted.
  - *TLS*: add a `tls` block (and/or `sslmode=require` in the DSN) to secure the channel.
- **Authentication combinations**
  - *Username/password*: provided via the DSN userinfo or `options.credentials`.
  - *Client certificate*: configured via `tls.cert` and `tls.key`, which mandates TLS.
- **Warm-up expectations**
  - Enforce `pool.min_connections` during warm-up and at runtime. Dropping below the threshold triggers circuit breaker logic and transitions dependent routes to `NOT_READY`.

#### **gRPC Connector**
- Keys:
  - `endpoint`: required, base URL for the target service (`http://` or `https://`).
  - `descriptor`: optional path to a compiled protobuf descriptor set that phases may reuse.
  - `metadata`: optional map of default metadata headers (ASCII keys) attached to every request.
  - `tls`: optional object providing `ca`, `cert`, `key`, and `domain` for mutual TLS setups.
- `timeouts.connect` and `timeouts.request` apply to the tonic `Endpoint`, setting the default connection handshake and unary request deadlines shared by all `grpc_client` phases that target the connector.
- Used with `grpc_client` phases to invoke unary RPCs.
- **Transport combinations**
  - *Plaintext*: supply an `http://` endpoint and omit the `tls` block.
  - *TLS*: use `https://` and configure the `tls` object; include `cert`/`key` for mutual authentication.
- **Authentication combinations**
  - *Metadata*: embed API tokens or auth headers via the `metadata` map.
  - *Client certificate*: provide `tls.cert` and `tls.key`; TLS must be enabled.

#### **RabbitMQ Connector**
- Keys:
  - `url`: required AMQP 0.9.1 URI (e.g., `amqps://user:pass@host/vhost`).
  - `prefetch`: optional integer for QoS prefetch (defaults to the broker/client default of `0`, meaning unlimited).
  - `tls`: optional nested object for certificates.
- Supports both inbound consumers (triggers) and outbound publishers (phases).
- **Transport combinations**
  - *Plaintext*: use `amqp://` URIs and omit the `tls` block.
  - *TLS*: use `amqps://` or add `tls.ca`/`tls.cert`/`tls.key`.
- **Authentication combinations**
  - *Username/password*: embed credentials in the URI or `options.credentials`.
  - *Client certificate*: specify `tls.cert`/`tls.key`; TLS must be active.
- **Operational tuning**
  - `prefetch` sets the default QoS prefetch for every consumer created from the connector. Individual triggers may override the value.
  - Additional retry fields such as `retry_initial`, `retry_max`, and `retry_multiplier` can be supplied inside the connector `options` map (or `options.extra`) to establish global backoff behaviour for all downstream triggers.

**TLS/Auth matrix**

| Transport mode | Authentication | Required options | Notes |
|----------------|----------------|------------------|-------|
| Plaintext      | None           | `url: amqp://…`  | Suitable for local development when brokers do not require auth. |
| Plaintext      | Username/password | `url: amqp://user:pass@…` | Credentials are embedded directly into the URI. |
| TLS (server auth) | Username/password | `url: amqps://user:pass@…`, `tls.ca` | `tls.ca` must point to a CA bundle for peer verification. |
| Mutual TLS     | Client certificate | `url: amqps://…`, `tls.ca`, `tls.cert`, `tls.key` | Optional username/password may still be embedded alongside mTLS. |

#### **MQTT Connector**
- Keys:
  - `url`: required broker URL (`mqtt://`, `mqtts://`).
  - `client_id`: optional client identifier.
  - `username`, `password`: optional credentials.
  - `tls`: optional nested TLS configuration. When supplying client certificates, place the concatenated certificate/key PEM in `tls.cert`; Chronicle validates `tls.key` if provided, but the MQTT client runtime expects the combined file path.
  - `keep_alive`: optional seconds (defaults to the client runtime's `60s` keep-alive interval).
- Usable for MQTT subscriptions (triggers) and publications (phases).
- **Transport combinations**
  - *Plaintext*: `mqtt://` URL with no `tls` block.
  - *TLS*: `mqtts://` URL and/or populated `tls.ca`/`tls.cert`/`tls.key`.
- **Authentication combinations**
  - *Username/password*: use the explicit `username` and `password` fields.
  - *Client certificate*: provide the TLS cert/key pair (must run over TLS).
- **Operational tuning**
  - `keep_alive` configures the MQTT keep-alive interval (seconds) used by Chronicle's subscriber and publisher clients.
  - Optional retry settings (`retry_initial`, `retry_max`, `retry_multiplier`) establish default reconnect backoff parameters for all triggers referencing the connector; triggers can override them on a per-subscription basis.

**TLS/Auth matrix**

| Transport mode | Authentication | Required options | Notes |
|----------------|----------------|------------------|-------|
| Plaintext      | None           | `url: mqtt://…` | For unsecured brokers; leave `tls` unset. |
| Plaintext      | Username/password | `url: mqtt://…`, `username`, `password` | Often used when brokers sit behind a local network. |
| TLS (server auth) | Username/password | `url: mqtts://…`, `tls.ca`, `username`, `password` | `tls.ca` is required to trust the broker certificate. |
| Mutual TLS     | Client certificate | `url: mqtts://…`, `tls.ca`, `tls.cert`, `tls.key` | `client_id`/`keep_alive` remain optional; username/password may be omitted. |

#### **SMTP Connector**
- Keys:
  - `host`: required SMTP host name.
  - `port`: optional integer port; defaults to `587` when omitted.
  - `auth`: optional nested object with `username`, `password`, and optional `mechanism`.
  - `tls`: optional nested object describing the connection: `mode` (`starttls`, `tls`, or `none`), `ca`, `domain`, `cert`, and `key`.
  - `from`: optional default envelope sender used when a phase omits the field.
  - `timeout`: optional duration applied to socket operations.
  - Additional keys may be supplied and flow into the connector's `extra` map.
- Supports `smtp_send` phases which enqueue messages with per-phase overrides for subject, recipients, and body templates.
- **Transport combinations**
  - *STARTTLS*: set `tls.mode: starttls`; Chronicle issues `STARTTLS` after negotiating the plain connection.
  - *TLS (implicit)*: set `tls.mode: tls`; the connection is established over TLS from the first byte (useful with port `465`).
  - *Plaintext*: omit `tls` or set `tls.mode: none` for local or test deployments. Combine with `timeout` to bound retries.
- **Authentication combinations**
  - *Username/password*: supply the `auth` object; `mechanism` defaults to driver heuristics (typically `plain`).
  - *Client certificate*: pair `tls.mode: tls` with `tls.cert`/`tls.key` and optional `tls.ca`/`tls.domain` for mutual TLS.
- Chronicle resolves the `tls` file paths relative to the integration file location, matching the behaviour of other connector TLS options.

#### **MongoDB Connector**
- Keys:
  - `uri`: required MongoDB connection string.
  - `database`: required default database.
  - `options`: optional nested driver overrides.
  - `tls`: optional nested TLS configuration.
- Powers MongoDB phases (CRUD/aggregation) and change-stream triggers (when enabled).
- **Transport combinations**
  - *Plaintext*: `mongodb://` plus no TLS overrides.
  - *TLS*: `mongodb+srv://` or `options.tls` with CA/cert/key.
- **Authentication combinations**
  - *Username/password*: embed credentials inside the URI or `options.auth`.
  - *Client certificate*: populate `tls.cert`/`tls.key`, which requires TLS.

#### **Redis Connector**
- Keys:
  - `url`: required Redis URI (`redis://` or `rediss://`).
  - `cluster`: optional boolean indicating cluster mode (defaults to `false`).
  - `tls`: optional nested TLS configuration.
  - `pool`: optional pool sizing information.
- Supports Redis Pub/Sub, streams, and key/value operations.
- **Transport combinations**
  - *Plaintext*: `redis://` URIs with no `tls` section.
  - *TLS*: `rediss://` URIs and/or `tls.ca` plus optional `tls.cert`/`tls.key`.
- **Authentication combinations**
  - *Username/password*: embed credentials in the URI or supply `credentials.username`/`credentials.password`.
  - *Client certificate*: configure `tls.cert`/`tls.key`, which implies TLS transport.

### 2.3 Validation Rules
- Connector names must be unique.
- Connector type must match supported drivers.
- Missing or malformed options cause startup failure.
- Validation aggregates connector and route conflicts instead of failing fast. When two routes attempt to share an incompatible connector (e.g., idempotent + best-effort or producer + consumer), `chronicle validate` lists every offending route/phase path in a single run so the operator can fix all references together.
- Any connector that references a client certificate (`tls.cert`/`tls.key` or driver-specific client-cert fields) MUST also enable TLS transport (HTTPS, SSL, `rediss`, etc.); configurations that violate this combination are rejected.
- Triggers must reference connectors configured for the correct role/protocol (e.g., HTTP triggers can only target `role = server` connectors, Kafka triggers/phases must reference Kafka connectors configured with the required broker/security settings).
- HTTP/S connectors using `https://` `base_url` values or TLS-enabled listeners must provide the trust material (CA or system roots) necessary to validate peers; plaintext connectors may not declare HTTPS URLs.
- Each phase/trigger must provide the connector-specific required fields (topic, queue, SQL, etc.); validation fails if mandatory keys for the referenced connector type are omitted.
- Duration fields must be strictly greater than zero. Validation errors include the offending field path (e.g., `app.retry_budget.max_backoff must be > 0`).
- When TLS client certificates are provided but the connector scheme remains plaintext, validation errors cite `connector.<name>.tls requires TLS transport`.
- Parallel phases using `aggregation: quorum` must satisfy `1 <= quorum <= len(children)`; otherwise Chronicle emits `parallel.<name>.options.quorum out of range`.
- Redis connectors running in `cluster: true` mode with TLS settings must provide `tls.ca` to avoid silent trust of system roots. Missing material surfaces as `connector.<name>.tls.ca required for cluster+TLS`.
- Kafka routes or phases that set `delivery.idempotent: true` automatically require `acks=all` and `max.in.flight.requests.per.connection <= 5`. The loader enforces those overrides and logs a validation hint if the connector attempts to loosen them. Because the enforcement occurs on the shared connector handle, validation also ensures that connectors referenced by idempotent routes are not simultaneously used by non-idempotent routes; declare separate connectors if you need both behaviours.
- MongoDB connectors using `mongodb+srv://` or any connector that enables TLS while supplying client certificates must also provide trust material (`tls.ca`) unless the runtime is explicitly configured to use system roots. Chronicle rejects connectors that attempt to run in TLS mode without peer verification.
- Connectors that support hostname verification flags (`tls.mode: verify_full`, `tls.domain`, etc.) must provide the corresponding DNS name or SNI override; omitting it while requesting strict verification is a validation error.
- Route-level limit overrides must not exceed application defaults. The loader enforces `policy.limits.max_inflight ≤ app.limits.routes.max_inflight`, `policy.limits.http.max_concurrency ≤ app.limits.http.max_concurrency`, and transport-specific analogues (`policy.limits.kafka.max_inflight_per_partition`, etc.).
- Connectors may not be reused across incompatible roles or delivery policies. For example, a Kafka connector cannot simultaneously serve `producer` and `consumer` roles, and an idempotent Kafka route cannot share a connector with a best-effort route. The loader emits `connector.<name> cannot be shared between conflicting roles/policies` when it detects such reuse.

### 2.4 Warm-Up, Health, and Backpressure Expectations
- Connectors used as Endpoints MUST honour `warmup` and `warmup_timeout` options. Drivers establish the minimum viable connection pool, handshake, or producer metadata before the route may report READY.
- Circuit breakers (`cb`) operate per Endpoint instance with standard Closed → Open → Half-Open → Closed transitions. Failure rates are measured over rolling windows; hard failures (connection refused, pool below `min_connections`) trip the breaker immediately. Open intervals back off exponentially with jitter and remain capped by `open_max`.
- Continuous background reconnection loops run irrespective of readiness so that recovery is proactive. When a breaker transitions to Half-Open, limited concurrent probes test the dependency before closing the breaker.
- Connectors may declare `retry_budget` blocks that intersect with application and route budgets. The tightest budget always wins, ensuring that noisy downstream systems cannot exhaust the global retry envelope.
- Warm-up retries follow the same retry budget precedence: the runtime uses the connector-level `retry_budget` when present, otherwise the application default, to compute the jittered exponential delays between warm-up attempts. The initial warm-up backoff starts at `app.retry_budget.base_backoff`, doubles until `max_backoff`, and then saturates—once the upper bound is reached, subsequent attempts wait exactly `max_backoff` (plus jitter) even if the loop continues for minutes. After every complete warm-up attempt Chronicle resets the `max_elapsed` stopwatch, so the elapsed guard applies per attempt while the cumulative warm-up duration remains unbounded; warm-up therefore continues until a connection finally succeeds or the operator shuts the process down.
- Per-connector `limits` (e.g., Kafka `max_inflight_per_partition`) are enforced before route-level backpressure to keep resource usage predictable. Enforcement counters surface via `chronicle_limits_enforced_total`.
- When a connector acts as an inbound listener (HTTP server, Kafka consumer), it must respect route readiness:
  - HTTP servers surface route state via immediate 503 responses that include `Retry-After`.
  - Kafka consumers pause affected topic/partition assignments until the route recovers.
- Warm-up completion is a prerequisite for changing readiness. No connector or endpoint may lazily establish resources on the first live request.

### 2.5 Validation Guarantees at a Glance

| Rule | Enforcement stage | Outcome |
| --- | --- | --- |
| `policy.limits.* ≤ app.limits.*` | Schema validation (`chronicle validate`) | Configuration error with the offending field path. |
| `retry_budget.max_backoff > retry_budget.max_elapsed` | Schema validation | Configuration error. |
| `retry_budget.max_backoff = retry_budget.max_elapsed` | Schema validation | Warning with remediation hint; load still succeeds. |
| Connector reused across incompatible roles/policies | Schema validation | Configuration error; load aborts before runtime. |
| `policy.delivery.retries` (or phase override) exceeds the tightest `retry_budget.max_attempts` | Schema validation | Warning surfaced during `chronicle validate`; runtime still clamps attempts. |

These guarantees keep the runtime, the CLI validator, and the JSON Schema aligned so that misconfigurations are caught before deployment.

---

## 3. Chronicles

### 3.0 Chronicle Field Summary

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `name` | string | — | Unique route identifier; reused in metrics/trace labels and `/status.routes[].id`. |
| `trigger.connector` | string | — | Must reference a connector with the correct role/type (HTTP server, Kafka consumer, etc.). |
| `trigger.options` | map | — | Transport-specific trigger configuration (HTTP method/path, Kafka topic/group, etc.). |
| `phases[]` | array | — | Ordered steps. Transforms should appear before side-effecting phases to keep retries deterministic. |
| `policy.requires` | array | inferred dependency set | Overrides automatic dependency detection when a phase encapsulates hidden fan-outs. |
| `policy.limits.*` | map | inherits from `app.limits` | Per-route ceilings; schema ensures every value is ≤ its app-level counterpart. |
| `policy.retry_budget.*` | object | inherits from tighter of `app`/connector budgets | Narrows or widens retries but can never loosen connector/app constraints. |
| `policy.delivery.*` | object | — | Shared delivery policy (`retries`, `backoff`, `idempotent`, `tx`). Phase-level overrides inherit from this block. |
| `policy.half_open_counts_as_ready` | enum | `app.half_open_counts_as_ready` | Route-specific readiness posture for half-open breakers. |
| `policy.allow_partial_delivery` | bool | `false` | Enables DEGRADED processing when paired with `fallback` or `optional` phases. |
| `policy.fallback` | map | — | Maps dependency IDs to fallback phases/routes. Required when `allow_partial_delivery=true` and the dependency must be rerouted. |
| `policy.readiness_priority` | int | configuration order | Determines which routes satisfy numeric `min_ready_routes` thresholds first. |

### 3.1 Structure
```yaml
- name: <string>
  trigger: <trigger-definition>
  phases: <list of phases>
  policy: <route-policy>
```

| Field | Type | Required | Description |
|-------|------|-----------|-------------|
| `name` | string | yes | Unique chronicle name. |
| `trigger` | object | yes | Defines the initiating event source. |
| `phases` | array | yes | Ordered list of phase definitions. |
| `policy` | object | no | Route-level overrides for readiness and delivery behaviour. |

### 3.2 Trigger Definition
```yaml
trigger:
  connector: <connector_name>
  options: <map>
```

The connector's declared `type` determines the trigger behaviour; specifying a `type`
inside the trigger block was deprecated and is now rejected by the loader.

#### **HTTP Trigger**
- Connector must be `type: http` with `role = server`.
- `options` keys:
  - `method`: required HTTP method.
  - `path`: required request path.
  - `content_type`: required accepted content type.
  - `max_body_bytes`: optional override (must be ≤ `app.limits.http.max_payload_bytes`); use it to reserve smaller buffers per route.
  - Additional request guards (e.g., `auth`, `max_body_bytes`) may be provided.
- Slot `.[0]` receives the inbound request as structured JSON:
  - `headers`, `query`, and `path` objects for request metadata.
  - `body` for the parsed payload (JSON or form-encoded).
  - `metadata.*` for connection details (remote address, received_at, user agent, etc.).
- Path templates follow Axum-style extraction (`/orders/{order_id}` populates `.[0].path.order_id`). Chronicle validates that required path parameters are referenced in the template; missing placeholders cause loader errors.

#### **Kafka Trigger**
- Connector must be `type: kafka`.
- `options` keys:
  - `topic`: required topic name.
  - `group_id`: required consumer group.
  - `auto_offset_reset`: optional, `earliest` or `latest`.
  - `transform`: optional jaq filter to pre-shape the consumed record.
- Slot `.[0]` receives the consumed record as structured JSON with `key`, `value`, `headers`, `topic`, `partition`, `offset`, and `timestamp`.

#### **RabbitMQ Trigger**
- Connector must be `type: rabbitmq`.
- `options` keys:
  - `queue`: required queue name.
  - `ack_mode`: optional (`auto`, `manual`).
  - `prefetch`: optional override per consumer.
- `retry_initial`: optional lower bound for the exponential backoff applied after connection errors; defaults to `200ms`.
- `retry_max`: optional upper bound for the same backoff; defaults to `5s` and is coerced to be at least `retry_initial`.
- `retry_multiplier`: optional growth factor (float between 1.1 and 10.0) applied to the backoff window; defaults to `2.0`.
- These reconnect settings are still intersected with the effective retry budget: Chronicle uses the connector-provided `retry_initial`/`retry_max` unless the budget would be violated, in which case it clamps to the tighter window and applies the budget's jitter rules.
- Slot `.[0]` receives AMQP delivery metadata: `body`, `headers`, `properties` (correlation_id, content_type, etc.), `routing_key`, `exchange`, `delivery_tag`, and `redelivered`.
- Legacy keys `retry_initial_ms`/`retry_max_ms` are rejected when `api_version: v1` is declared; convert to the duration-based fields above.

#### **MQTT Trigger**
- Connector must be `type: mqtt`.
- `options` keys:
  - `topic`: required subscription topic or filter.
  - `qos`: optional quality-of-service (0, 1, 2).
  - `retain_handling`: optional behavior for retained messages.
- `retry_initial`, `retry_max`, `retry_multiplier`: optional overrides for the per-trigger reconnect backoff; each mirrors the semantics documented for RabbitMQ triggers.
- Chronicle intersects these values with the effective retry budget (app/connector/route) so reconnect attempts never exceed `max_attempts` or `max_elapsed`, and jitter always matches the most conservative setting.
- Slot `.[0]` contains `topic`, `payload`, `qos`, `retain`, and `metadata.timestamp`.
- As with RabbitMQ, the `_ms` suffixed retry keys are deprecated and removed in `api_version: v1`.

#### **MongoDB Trigger**
- Connector must be `type: mongodb` and configured for change streams.
- `options` keys:
  - `collection`: optional, defaults to whole database stream.
  - `pipeline`: optional aggregation pipeline array applied to the stream.
- Slot `.[0]` receives the change event document, including `operationType`, `ns`, `documentKey`, and `fullDocument` (when requested).

#### **Redis Trigger**
- Connector must be `type: redis`.
- `options` keys:
  - `mode`: `pubsub` or `stream`.
  - `channels`: required list for `pubsub`.
  - `stream`: required stream name for `stream` mode.
  - `group`: optional consumer group for streams.
- Slot `.[0]` contains `channel`/`stream`, `payload`, `id` (for streams), `metadata.timestamp`, and `attributes` for message fields.

### 3.3 Route Policy and Dependency Sets
- Each chronicle instantiates a Route that binds its Trigger connector to all outbound Endpoint connectors referenced in phases. This automatically detects the Dependency Set.
- Optional `policy` fields allow fine control:
  ```yaml
  policy:
    requires: ["db-main", "kafka-out"]   # explicit dependency set override
    allow_partial_delivery: false        # default; set to true only with explicit fallbacks
    readiness_priority: 10               # lower numbers evaluated first
    limits:
      max_inflight: 256
      overflow_policy: queue             # queue|reject|shed
    retry_budget:
      max_attempts: 4
      max_elapsed: 10s
      base_backoff: 100ms
      max_backoff: 5s
      jitter: equal
    half_open_counts_as_ready: never
    delivery:
      retries: 5
      backoff: "50ms..2s"
      idempotent: true
  ```
- `requires` supersedes inferred dependencies when a phase encapsulates multiple logical endpoints (e.g., SQL fan-out).
- `allow_partial_delivery` is `false` by default. When set to `true`, `fallback` mappings must define how missing endpoints are handled; otherwise the readiness controller blocks new work.
- `fallback` entries can reference alternate routes or phase labels (e.g., `fallback: {"kafka-out": "dead-letter-phase"}`) that receive messages when the primary endpoint is unavailable. Parallel phases integrate with this policy through per-child `optional` and `fallback` markers.
- When `allow_partial_delivery: true` is enabled, every required outbound dependency must either (a) declare an explicit `fallback` (dead-letter, alternate region, etc.) or (b) be flagged `optional: true` inside the parallel phase that invokes it. Chronicle warns during validation if a dependency lacks both, because a hard failure would still stop the route rather than entering `DEGRADED`. At runtime, if a required dependency fails without fallback, the parent route transitions to `NOT_READY` even when `allow_partial_delivery` is true.
- `limits` overrides inherit from `app.limits.routes` and allow per-route tuning of concurrency and overflow behaviour.
- `retry_budget` narrows or expands the application default. Budgets compose as the minimum across app, connector, and policy scopes; omitting the block defers entirely to broader defaults.
- `retry_budget` narrows or expands the application default. Budgets compose as the minimum across app, connector, and policy scopes; omitting the block defers entirely to broader defaults. `policy.delivery.backoff` only defines the cadence of attempts; the resulting schedule is still clamped by the effective retry budget so a route can never run "extra" retries beyond its envelope.
- `half_open_counts_as_ready` enables per-route overrides for readiness evaluation, mirroring the application field.
- `readiness_priority` determines the deterministic ordering used when `app.min_ready_routes` is an integer. Lower values indicate higher priority; ties fall back to configuration order.
- Delivery policies mirror the per-phase settings but allow a single definition per Route. Absent values fall back to phase defaults.
- When `delivery.idempotent: true` is declared, Chronicle now forces any Kafka producers referenced by the Route to enable broker-side idempotence (setting `enable.idempotence=true`, `acks=all`, and `max.in.flight.requests.per.connection<=5`). Because Kafka producer handles are shared per connector (Section 1.1), this enforcement happens at the connector level. As a result, a connector cannot simultaneously serve idempotent and non-idempotent routes; validation rejects the configuration unless those routes use distinct connectors or all of them opt in to idempotent delivery.
- Dependency Sets are discovered at configuration load by analysing static references to `options.connector` across all phases, including nested parallel children. Dynamic connector selection is not supported; configs requiring runtime switching should model the variants as separate phases with explicit fallbacks. Typical patterns include a `parallel` phase with two child endpoints (`optional: true` on the backup) plus `policy.fallback` entries that reroute to a dead-letter connector when the primary is unhealthy, or two explicit sequential phases guarded by `allow_partial_delivery` so regional failover remains declarative.
- Routes expose their state (`WARMING_UP`, `READY`, `NOT_READY`, `DEGRADED`, `DRAINING`) to the readiness controller and management APIs.

---

## 4. Phase Definition

### 4.1 Schema
```yaml
- name: <string>
  type: <string>
  options: <map>
```

| Field | Type | Required | Description |
|-------|------|-----------|-------------|
| `name` | string | yes | Identifier of the phase within the chronicle. |
| `type` | string | yes | Executor type (`transform`, `http`, `kafka`, `mariadb`, `parallel`, etc.). |
| `options` | map | yes | Phase-specific configuration with jaq expressions. |
| `timeout` | string | no | Maximum execution time for outbound work (overrides child timeouts when `type = parallel`). Durations use Go-style notation (e.g., `250ms`, `5s`, `2m`). |

**Timeout semantics**
- Outbound executors (`http`, `kafka`, `mariadb`, `postgres`, `rabbitmq`, `mqtt`, `mongodb`, `redis`) accept `timeout`. Absence means they rely on the connector's defaults.
- Parallel phases also accept `timeout`, which caps total fan-out duration regardless of child settings.

Each phase consumes values from prior slots (e.g., `.[0].body.record.attributes.category`) and produces a structured JSON output assigned to its slot index (`.[n]`).

### 4.2 Payload Codecs
- Chronicle now treats payload encoding as a dedicated concern. Encoded payloads are represented as structured JSON envelopes containing `codec`, `content_type`, `binary`, `length`, and codec-specific metadata (e.g., `wire_format`, `schema_id`, `descriptor_path`).
- The `serialize` phase uses the shared codec registry (`src/codec/`) to emit Avro, Protobuf, or CBOR payloads, while the default behaviour is equivalent to `codec: json`. Enabling this phase requires the `serialize_phase` feature flag; without it, configs referencing `type: serialize` fail to load. Downstream transports (HTTP, Kafka, gRPC, etc.) always understand codec envelopes even if the serialize flag is disabled because other phases may still supply encoded payloads.
- HTTP client phases and the action dispatcher automatically detect these envelopes. When a payload declares a codec, Chronicle streams the binary body and applies the advertised `Content-Type`. When no codec is present it falls back to serialising structured JSON as `application/json`.
- Kafka publishes follow the same rules: encoded payloads pass through unchanged, while plain JSON payloads are serialised with the default codec. This keeps HTTP, Kafka, and gRPC payload semantics aligned.
- Codec defaults:

| Codec | Default `Content-Type` | Envelope fields | Schema / descriptor hint | Notes |
| --- | --- | --- | --- | --- |
| `json` | `application/json` | `codec`, `content_type`, `binary=false`, `length` | Optional `schema_id` (free-form) | Default when no codec is declared; bodies remain structured JSON. |
| `avro` | `application/avro` | `codec`, `content_type`, `binary=true`, `schema_id`, `wire_format=avro` | `schema_id` or `schema_path` must resolve to schema registry or bundled schema | Supports Confluent-style registry IDs or local file references. |
| `protobuf` | `application/x-protobuf` | `codec`, `content_type`, `binary=true`, `descriptor_path`, `wire_format=proto` | `descriptor_path` (relative to config) or connector-level descriptor | Chronicle merges descriptor metadata from the connector when the field is omitted. |
| `cbor` | `application/cbor` | `codec`, `content_type`, `binary=true`, `wire_format=cbor` | Optional `schema_id` for tooling | Binary bodies stream directly to transports; ideal for IoT payloads. |

- Additional codecs (e.g., MsgPack) can now plug into the registry without touching transport-specific code; CBOR ships alongside Avro and Protobuf as a built-in option.

---

## 5. Phase Types
Transformation pipelines should be composed of pure data-mapping steps followed by isolated side-effecting Endpoint interactions. This keeps retries deterministic and allows the readiness controller to reason about dependency health independently of business logic.

### 5.1 Transform Phase
Performs field mapping and data synthesis using jaq selectors.
```yaml
type: transform
options:
  summary:
    record_id: .[0].body.record.id
    category: .[0].body.record.attributes.category
    latency_ms: .[0].body.record.metrics.latency_ms
  trace:
    id: .[0].headers.trace_id
    received_at: .[0].metadata.timestamp
```
- Reads: arbitrary values from existing slots via jaq selectors.
- Writes: JSON object defined by the `options` map into slot `.[n]`.

### 5.2 Kafka Phase
Publishes Kafka records through a shared connector.
```yaml
type: kafka
timeout: 1500ms
options:
  connector: <kafka_connector>
  topic: <string>
  acks: <all|leader|none>        # producer-only
  key: .[0].body.record.id
  body:
    payload: .[0].body.record
    trace_id: .[0].headers.trace_id
```
- Reads: payload fragments from earlier slots (`key`, `headers`, `body`, optional `partition`).
- Writes: delivery metadata (`topic`, `partition`, `offset`, `timestamp`) into slot `.[n]`.
- Honors `timeout`, aborting the publish if the broker does not acknowledge within the allotted window.
- When `delivery.idempotent: true` is inherited from the route or set on the phase, Chronicle enforces `enable.idempotence=true`, `acks=all`, and `max.in.flight.requests.per.connection<=5` regardless of the connector defaults to avoid out-of-order retries.
- Per-phase overrides that conflict with idempotent requirements (for example, `acks: leader`) are rejected during validation with `phase "<chronicle>/<phase>" cannot override acks when delivery.idempotent=true`.

### 5.3 HTTP Phase
Issues outbound HTTP requests using an `http` connector.
```yaml
type: http
timeout: 2s
options:
  connector: <http_connector>
  method: POST
  path: /api/v1/send
  headers:
    trace_id: .[0].headers.trace_id
  body:
    record: .[0].body.record
    summary:
      category: .[0].body.record.attributes.category
      latency_ms: .[0].body.record.metrics.latency_ms
```
- Reads: jaq expressions provide request parts (`path`, `query`, `headers`, `body`, `auth` tokens, etc.) from prior slots.
- Writes: response context into slot `.[n]`, including `status`, `headers`, `body`, `duration`, and transport metadata.
- `timeout` bounds the end-to-end HTTP client call, including TLS handshakes.

### 5.4 MariaDB Phase
Executes SQL against a MariaDB connector with jaq-bound parameters.
```yaml
type: mariadb
timeout: 3s
options:
  connector: <mariadb_connector>
  sql: |
    INSERT INTO records (record_id, category, observed_at, latency_ms)
    VALUES (:record_id, :category, :observed_at, :latency_ms)
  values:
    record_id: .[0].body.record.id
    category: .[0].body.record.attributes.category
    observed_at: .[0].body.record.observed_at
    latency_ms: .[0].body.record.metrics.latency_ms
  idempotency:
    key: .[0].body.record.id
    on_conflict: update
    conflict_target: record_id
    update_set:
      category: .[1].category
      observed_at: .[1].observed_at
      latency_ms: .[1].latency_ms
```
- Reads: parameter bindings sourced from `values.*`, where each dotted key maps directly to a named SQL parameter (e.g., `values.category` supplies `:category`).
- Writes: slot `.[n]` receives `rows` (affected-row count) and `results` (array of row objects for SELECT/RETURNING statements).
- `timeout` cancels the database call if it exceeds the configured window.
- The optional `idempotency` block declares a deterministic key and conflict policy. Supported `on_conflict` values are `ignore` and `update`; `update_set` maps directly to SQL assignments executed when a conflict is detected.
- Routes or phases that set `delivery.idempotent: true` must provide the `idempotency` block (or reference a stored procedure that implements the same semantics). Database transactions (`policy.delivery.tx: true`) do not, by themselves, guarantee idempotence across retries because Chronicle may replay the phase after a timeout; the loader therefore insists on an explicit key. Validation fails with `phase "<chronicle>/<phase>" must declare idempotency.key when delivery.idempotent=true`.
- When `on_conflict: update` is selected, `update_set` must contain at least one column/value pair; empty updates are rejected at load time to guarantee the retried statement performs real work.
- `idempotency.key` selectors must resolve to non-empty strings or byte arrays; Chronicle validates the expression at load time and raises `idempotency.key must resolve to a non-empty scalar` if the selector could produce `null` or structured data.
- **Example (Idempotent Postgres upsert with retries)**
  ```yaml
  policy:
    delivery:
      retries: 4
      backoff: "200ms..2s"
      idempotent: true
  phases:
    - name: persist_order
      type: postgres
      timeout: 1500ms
      options:
        connector: db-orders
        sql: |
          INSERT INTO orders (order_id, status, payload)
          VALUES (:order_id, :status, :payload::jsonb)
        values:
          order_id: .[0].body.order.id
          status: .[0].body.order.status
          payload: .[0].body.order
        idempotency:
          key: .[0].body.order.id
          on_conflict: update
          conflict_target: order_id
          update_set:
            status: .[0].body.order.status
            payload: .[0].body.order
  ```
  Chronicle now retries the phase up to four times, but the idempotent insert ensures repeated attempts update the same row deterministically instead of creating duplicates.

### 5.5 Postgres Phase
Executes SQL against a Postgres connector with jaq-bound parameters.
```yaml
type: postgres
timeout: 3s
options:
  connector: <postgres_connector>
  sql: |
    INSERT INTO records (record_id, category, observed_at, latency_ms)
    VALUES (:record_id, :category, :observed_at, :latency_ms)
  values:
    record_id: .[0].body.record.id
    category: .[0].body.record.attributes.category
    observed_at: .[0].body.record.observed_at
    latency_ms: .[0].body.record.metrics.latency_ms
  idempotency:
    key: .[0].body.record.id
    on_conflict: update
    conflict_target: record_id
    update_set:
      attributes: .[1].attributes
      metrics: .[1].metrics
```
- Reads/Writes: identical semantics to the MariaDB phase, including honoring `timeout`.
- Postgres also supports `on_conflict: do_nothing` and `constraint: <name>` when the underlying table defines a named unique constraint. Chronicle validates that idempotency keys are supplied whenever delivery semantics require retries.
- When an enclosing route or phase requests `delivery.idempotent: true`, the Postgres phase must declare `idempotency.key` plus either `on_conflict` or `constraint`. Transactions (`delivery.tx: true`) without conflict handling are insufficient because retries can still re-run the statement. Missing fields produce `phase "<chronicle>/<phase>" must declare idempotency.* when delivery.idempotent=true`.
- `on_conflict: update` additionally requires a non-empty `update_set` map so Chronicle can deterministically merge retries into the existing row.
- As with MariaDB, the key selector must produce a non-empty string/bytes payload; structured or empty results trigger validation errors because they would make deduplication ambiguous.

### 5.6 RabbitMQ Phase
Publishes messages to RabbitMQ exchanges or queues.
```yaml
type: rabbitmq
timeout: 1500ms
options:
  connector: <rabbitmq_connector>
  exchange: my.exchange
  routing_key: records.processed
  headers:
    trace_id: .[0].headers.trace_id
  body:
    record: .[0].body.record
    metrics: .[0].body.record.metrics
  properties:
    delivery_mode: persistent
```
- Reads: message body, headers, and properties assembled from jaq expressions.
- Writes: slot `.[n]` captures broker acks such as `delivery_tag`, `exchange`, `routing_key`, and `timestamp`.
- Timeout cancels the publish attempt if the broker does not confirm in time.

### 5.7 MQTT Phase
Publishes MQTT messages through an MQTT connector.
```yaml
type: mqtt
timeout: 1s
options:
  connector: <mqtt_connector>
  topic: records.samples
  qos: 1
  retain: false
  payload:
    record: .[0].body.record
    observed_at: .[0].body.record.observed_at
```
- Reads: topic, payload, retained flag, and headers derived from prior slots.
- Writes: slot `.[n]` records acknowledgement metadata (`topic`, `qos`, `packet_id`, `timestamp`).
- Timeout bounds handshake plus publish acknowledgement.

### 5.8 MongoDB Phase
Executes MongoDB CRUD or aggregation operations.
```yaml
type: mongodb
timeout: 2.5s
options:
  connector: <mongodb_connector>
  collection: records
  operation: insert_one
  document: .[0].body.record
```
- Reads: operation-specific inputs such as `document`, `filter`, `update`, or `pipeline` sourced from jaq expressions.
- Writes: slot `.[n]` includes `inserted_id`, `matched_count`, `modified_count`, `deleted_count`, and `results` for queries/aggregations.
- Timeout caps the driver call (including server-side maxTimeMS when applicable).

### 5.9 Redis Phase
Executes Redis commands or publishes to channels/streams.
```yaml
type: redis
timeout: 750ms
options:
  connector: <redis_connector>
  command: set
  key: .[0].body.record.id
  value:
    summary:
      category: .[0].body.record.attributes.category
      latency_ms: .[0].body.record.metrics.latency_ms
```
- Reads: command name plus arguments (keys, values, fields) sourced from jaq expressions; publish modes support `channel`, `payload`, `stream`, and entry fields.
- Writes: slot `.[n]` stores command results (`status`, `value`, `stream_id`, etc.) and metadata such as `duration`.
- Timeout limits total command round-trip time.

### 5.10 Parallel Phase
Runs multiple child phases simultaneously and emits their combined results. This phase type remains behind the `parallel_phase` feature flag; configuration load fails with `enable app.feature_flags.parallel_phase to use type=parallel` when the flag is absent.
```yaml
type: parallel
timeout: 2.5s          # Optional cap for the entire fan-out
options:
  phases:
    - name: forward_summary
      type: http
      timeout: 1500ms
      options:
        connector: <http_connector>
        method: POST
        path: /fanout/summary
        body:
          summary: .[1].summary
          trace: .[1].trace
    - name: audit_record
      type: kafka
      timeout: 1.2s
      options:
        connector: <kafka_connector>
        topic: records.audit
        key: .[0].body.record.id
        body:
          payload: .[1]
```
- The `phases` array is required and holds inline child phases that use the standard schema but may not themselves be of type `parallel` (one level of nesting).
- Validation enforces the one-level rule: referencing `type: parallel` inside a child immediately raises `parallel.<name>.options.phases[i] may not be type=parallel`. This prohibition is absolute—even fallback or optional child definitions cannot embed another `parallel`, so each route may express only one level of fan-out per parent phase.
- All children receive the parent’s current context and execute concurrently. Parent success follows configurable aggregation rules declared via `options.aggregation`. Valid values are `all` (default), `any`, and `quorum`; validation rejects unknown values, refuses to accept `quorum` without an accompanying `options.quorum`, and enforces `1 ≤ quorum ≤ len(children)` so `quorum: 0` (or `quorum` above the child count) fails fast during `chronicle validate`. The default `all` mirrors the previous fail-on-any-child behaviour, while `aggregation: any` succeeds after the first child (or fallback) reports `success` and fails only when every child ultimately reports `failed` or `timeout`.
- When `aggregation: quorum` is selected, `options.quorum` specifies the minimum number of successful children required (defaults to `ceil(n/2)`).
- Parent `timeout` (if set) bounds the total fan-out duration and overrides child-specific limits.
- Each child may declare `optional: true` or `fallback` targets to coordinate with `allow_partial_delivery`. Optional children that fail emit `status: skipped` in the result map without failing the parent.
- Slot `.[n]` stores a `children` map keyed by child `name`. Each entry includes `status` (`success`, `failed`, `timeout`, or `skipped`), `output` (the child's result payload), and `error` metadata when applicable. Even when `aggregation: any` short-circuits after the first passing child, the parent slot still records every child entry so downstream phases and observability tools can inspect the full fan-out outcome.
- Chronicle serialises the `children` map in definition order (not completion order), so JSON diffs, tests, and automation that compare fan-out results remain deterministic even when concurrency completes children out of order.
- The parent emits `aggregation_state` (`success`, `partial`, `failed`) so routes can transition to `DEGRADED` when only optional children fail. Chronicle records partial successes in metrics to maintain observability parity with readiness signals.
- In the example, `forward_summary` posts the nested `summary`/`trace` bundle to an HTTP endpoint while `audit_record` publishes the same material to the `records.audit` Kafka topic, mirroring the behavior in `docs/reference/config.yaml`.
- Evaluation order:
  1. Chronicle executes each child with its own retry policy; child retries consume from the same effective retry budget as the parent, so optional children still burn attempts when they fail.
  2. Child completion yields a status: `success`, `failed` (exhausted retries or hard error), `timeout` (parent or child timeout hit), or `skipped` (optional child short-circuited by a dependency fallback).
  3. Fallback children declared via `fallback` run immediately after the primary child fails and consume from the same retry budget; their result replaces the primary's status for aggregation purposes.
  4. Aggregation counts `success` toward quorum, treats `failed` and `timeout` as failures, and ignores `skipped` entries. When `aggregation: quorum`, Chronicle requires `options.quorum` successes; otherwise a single failure causes the parent to fail (default `all`) or succeed (`any`) according to the configured mode.
  5. Parent `timeout` terminates all remaining children, marks them `timeout`, and evaluates the aggregation rules with the statuses gathered so far.

| Child outcome | Counts toward quorum? | Consumes retry budget? | Notes |
| --- | --- | --- | --- |
| `success` | Yes | Yes (attempts already spent) | Outputs available in `children.<name>.output` |
| `failed` | No (counts as failure) | Yes | Includes errors and exhausted retries |
| `timeout` | No (counts as failure) | Yes | Triggered by child or parent timeout |
| `skipped` (optional) | No | No additional attempts beyond those already spent | Route may still enter `DEGRADED` if optional children fail |
- Parallel child names never appear as metric labels; they are confined to the JSON result map (`children.<name>`) to prevent unbounded cardinality. Aggregation outcomes are surfaced through fixed-label counters such as `chronicle_parallel_children_total`.
- **Example (quorum + fallback + budgets)**  
  ```yaml
  policy:
    allow_partial_delivery: true
    delivery:
      retries: 3
      backoff: "100ms..1s"
    retry_budget:
      max_attempts: 3
      max_elapsed: 5s
      base_backoff: 100ms
      max_backoff: 1s
      jitter: full
  phases:
    - name: fanout
      type: parallel
      timeout: 2s
      options:
        aggregation: quorum
        quorum: 1
        phases:
          - name: primary_audit
            type: kafka
            timeout: 800ms
            options:
              connector: kafka-out
              topic: records.audit
              key: .[0].body.record.id
              body: .[0]
            fallback:
              connector: kafka-dlq
              topic: records.audit.dlq
          - name: regional_forward
            type: http
            optional: true
            timeout: 1s
            options:
              connector: http-secondary
              method: POST
              path: /regional/ingest
              body: .[0]
  ```
  `aggregation: quorum` with `quorum: 1` ensures the route succeeds if either the primary audit publish or the optional secondary HTTP call succeeds inside the shared 2 s timeout. Should `primary_audit` fail, its fallback publishes to the DLQ before the aggregation tally runs, consuming from the same retry budget as the parent. Optional `regional_forward` failures still burn retries (up to three attempts) but the parent only transitions to `DEGRADED` rather than failing outright because `allow_partial_delivery` is true.

- **Example (DEGRADED surface in `/status`)**  
  When `allow_partial_delivery: true` is paired with a failing optional child, the route completes but enters `DEGRADED` so operators know some fan-outs are unhealthy. The `/status` payload highlights the partial success while the child result map records `aggregation_state: "partial"`:
  ```json
  {
    "routes": [
      {"id":"r-fanout","state":"DEGRADED","requires":["kafka-out","http-secondary"],"unhealthy":["http-secondary"]}
    ],
    "endpoints": [
      {"id":"endpoint.http-secondary-client","type":"http-client","state":"CB_OPEN"}
    ]
  }
  ```
  Readiness recovers automatically once the optional dependency closes its breaker or a fallback succeeds.

### 5.11 gRPC Client Phase
Invokes unary RPCs described by a gRPC connector descriptor.
```yaml
type: grpc_client
timeout: 1500ms
options:
  connector: <grpc_connector>
  rpc: chronicle.ingest.v1.RecordIngress/Push   # or service + method
  descriptor: schemas/ingest.desc               # optional override
  project_response: true                        # optional summary projection
  request:
    recordId: .[0].body.record.id
    metrics: .[0].body.record.metrics
  metadata:
    x-trace-id: .[0].headers.trace_id
```
- Reads: the `request` object (or, when omitted, the previous slot) plus optional `metadata` entries resolved to strings. Metadata specified on the phase merges with defaults declared on the referenced gRPC connector.
- `rpc` is the preferred shorthand and must follow `<package.Service>/<Method>`; supplying `service` and `method` separately is also supported. `descriptor` is optional when the connector already declares one and otherwise required so the runtime can marshal payloads.
- Writes: slot `.[n]` receives either `null` (default) or a minimal response projection when `project_response: true`. Projection mode records `status`, `metadata` (with sensitive headers redacted), and `body_len` so downstream phases can branch on success/failure without parsing full protobuf messages.
- `timeout` bounds the full RPC, including name resolution and TLS handshakes. When omitted, the connector's default (or the transport's client-level timeout) applies.

### 5.12 SMTP Send Phase
Builds and emits an email envelope through an SMTP connector.
```yaml
type: smtp_send
options:
  connector: <smtp_connector>
  from: alerts@example.com            # falls back to connector default
  to:
    - ops@example.com
  cc: .[0].body.cc_list
  subject: >
    Record .[0].body.record.id exceeded latency SLO
  reply_to: notifications@example.com
  headers:
    X-Trace-Id: .[0].headers.trace_id
  body:
    text: >
      Record .[0].body.record.id observed latency of .[0].body.record.metrics.latency_ms ms.
  attachments:
    - filename: report.json
      content: .[1].encoded_report
      content_type: application/json
```
- Reads: addresses may be provided as strings (`"a@example.com,b@example.com"`) or arrays of strings; at least one recipient must be supplied across `to`, `cc`, or `bcc`. `from` defaults to the connector's `from` field when omitted. `body`, `headers`, and `attachments` are arbitrary templates resolved from prior slots, allowing richly formatted payloads or JSON-rendered summaries.
- Writes: slot `.[n]` stores a structured envelope summary (`from`, `subject`, `recipients`, `status: pending`, and the resolved `message` object) so downstream phases can log or branch on notification details. Delivery outcomes surface via metrics/logs rather than mutating the slot content.
- Timeouts are taken from the connector (`options.timeout`) and may be overridden per phase by supplying `timeout_secs` through the connector definition; SMTP phases themselves do not expose an additional `timeout` key.

### 5.13 Phase quick reference

| Phase type | Primary inputs | Output slot contents | Canonical errors surfaced |
| --- | --- | --- | --- |
| `transform` | `.[0]..` (trigger payload + prior slots) | Arbitrary JSON derived from jaq | `MISSING_KEY` when selectors fail |
| `http` | Request template built from any previous slot | HTTP response summary (`status`, `headers`, `body`, `duration`) | `TARGET_UNAVAILABLE`, `CONNECTOR_FAILURE`, `BUDGET_EXHAUSTED`, HTTP status mapping from Section 7 |
| `kafka` | Key/body/headers selectors plus route delivery policy | Broker ack metadata (`topic`, `partition`, `offset`, `timestamp`) | `TARGET_UNAVAILABLE`, `BUDGET_EXHAUSTED`, `CONNECTOR_FAILURE` |
| `mariadb` / `postgres` | SQL text + `values.*` bindings, optional `idempotency.*` | `rows`, `results`, driver metadata | `MISSING_KEY`, `CONNECTOR_FAILURE`, `TARGET_UNAVAILABLE`, schema constraint violations |
| `rabbitmq` / `mqtt` | Headers/properties/payload derived from prior slots | Broker confirmation metadata (`delivery_tag`, `packet_id`, `timestamp`) | `CONNECTOR_FAILURE`, `TARGET_UNAVAILABLE`, `BUDGET_EXHAUSTED` |
| `mongodb` | `document`, `filter`, or `pipeline` selectors | Operation result summary (counts, `inserted_id`, `results`) | `CONNECTOR_FAILURE`, `TARGET_UNAVAILABLE` |
| `redis` | Command arguments built from slots | Command result (`value`, `status`, `stream_id`) | `CONNECTOR_FAILURE`, `TARGET_UNAVAILABLE` |
| `grpc_client` | Marshalled protobuf request and metadata | Optional projection with status + metadata | `CONNECTOR_FAILURE`, `TARGET_UNAVAILABLE`, `BUDGET_EXHAUSTED` |
| `smtp_send` | Envelope addresses, headers, attachments | Pending envelope summary | `CONNECTOR_FAILURE`, `TARGET_UNAVAILABLE` |
| `parallel` | Entire slot context inherited from parent | `children.<name>` map + `aggregation_state` | Parent inherits child error codes according to aggregation rules |

Each executor follows the jaq semantics from Section 6: missing inputs fail fast, even when a child is marked optional. Use `allow_partial_delivery` and explicit fallbacks to continue despite downstream failures without relaxing validation.

---

## 6. Context Model and Reference Rules

### 6.1 Slot Indexing
| Slot | Description |
|------|--------------|
| `.[0]` | Trigger payload |
| `.[1]` | First phase output |
| `.[2]` | Second phase output |

### 6.2 Native JSON Context
- Each slot is a full JSON value (maps, lists, scalars) emitted directly by the trigger or phase—no flatten/unflatten passes are applied.
- Jaq expressions run against these native structures, so selectors like `.[0].body.record.attributes.category` work without prior transformation.
- Phase option assignments may use dotted keys (e.g., `body.metrics.latency_ms`) to describe where resolved values should appear in an output document; this does not affect the underlying context model.
- Numeric values preserve 64-bit integer precision by default; floating-point numbers use IEEE-754 double precision. Chronicle rejects inputs that overflow these bounds to avoid silent coercion.

### 6.3 Lookup Semantics
- All jaq references must resolve successfully.
- Unresolved keys cause immediate chronicle termination.
- Failures propagate upward: HTTP triggers return 400; Kafka consumers do not commit offsets.
- When inputs are optional, use jaq's `//` (default) operator or `try`/`catch` idioms to provide fallbacks (e.g., `.[0].headers.trace_id // "unknown"`). Chronicle still treats a selector that evaluates to `null` without a default as success; only missing paths raise `MISSING_KEY`.
- Even inside `optional: true` parallel children or best-effort fallbacks, jaq evaluation failures MUST bubble up as `MISSING_KEY`. The loader never inserts default values implicitly so operators can rely on deterministic validation.

---

## 7. Failure Semantics
- **Missing key**: fatal error, no continuation.
- **Connector resolution failure**: configuration error, prevents startup.
- **Runtime error in phase**: aborts chronicle execution, response depends on trigger type.
- **Retries**: automatically managed by Chronicle according to the tightest retry budget across app, connector, and route policy plus any `delivery.retries` settings. Failures propagate only after those configured attempts and backoffs are exhausted.
- **Warm-up failure**: endpoints that cannot initialise within `warmup_timeout` are marked `UNHEALTHY`, preventing readiness and keeping the application in `WARMING_UP` or `NOT_READY`.
- **Circuit breaker open**: route enters `NOT_READY` unless `allow_partial_delivery` and fallbacks are explicitly configured.

### 7.1 Circuit Breakers, Timeouts, and Recovery
- Circuit breakers apply per Endpoint instance and follow the Closed → Open → Half-Open → Closed lifecycle. Failures over the configured sliding window or hard dependency faults trip the breaker to `OPEN`.
- Open durations use exponential backoff with jitter between `open_base` and `open_max`. Half-open probes limit concurrent trial requests (between 1 and N) to verify recovery before closing.
- Every outbound call observes bounded timeouts. Combined with retry budgets (attempt ceilings, elapsed windows, jittered exponential backoff), this prevents indefinite hangs and feeds breaker failure metrics.
- Background reconnection loops run continuously, maintaining DB `pool.min_connections` and reestablishing Kafka producers/consumers. Falling below minimum pool size immediately marks the endpoint `UNHEALTHY` and triggers breaker logic.
- Circuit breaker state is isolated per Endpoint so failures do not cascade across unrelated Routes unless `min_ready_routes` requires all routes to be READY.

### 7.2 Transaction Handling Scenarios
- **HTTP → Kafka (Scenario 1):** If the Kafka producer endpoint is `UNHEALTHY` or `CB_OPEN`, `/ready` returns `503` and HTTP handlers reject requests within 50 ms using `Retry-After` plus body `{"error":"TARGET_UNAVAILABLE","route":"<id>","ts":"<rfc3339>"}`. No transformation or enqueue work runs while the endpoint is down. During warm-up the HTTP connector listens but always responds `503` until the producer becomes `HEALTHY`.
- **Kafka → DB (Scenario 2):** If the database pool falls below `min_connections`, mark the DB endpoint `UNHEALTHY`, trip the breaker, and pause Kafka consumption for the affected Route. `/ready` mirrors this state whenever the policy requires the Route for readiness. Warm-up starts consumers paused; readiness allows resume once the Endpoint recovers.
- **Consumer rebalances:** When an async trigger (Kafka, RabbitMQ, MQTT, Redis streams) rebalance occurs while Chronicle has the route paused, the new assignment is immediately re-paused before polling resumes. Chronicle re-applies the readiness check to each partition or subscription as it is assigned so offsets are never advanced while dependencies remain unhealthy.

### 7.3 In-Flight Work During Transitions
- When a Route transitions to `NOT_READY`, ongoing HTTP requests complete using the outcome returned by the endpoint, but no new requests are admitted.
- Kafka routes finish processing messages already polled (bounded by max in-flight limit) and stop fetching new messages while paused. Offset commits only happen after successful endpoint writes/transactions.

### 7.4 Error Taxonomy
- Errors surface canonical codes so operators can build uniform SLOs:
  - `TARGET_UNAVAILABLE`: dependency health prevents processing (readiness or breaker).
  - `MISSING_KEY`: jaq expression failed to resolve a required field.
  - `BUDGET_EXHAUSTED`: retry budget would be exceeded by another attempt.
  - `PAYLOAD_TOO_LARGE`: request exceeded configured payload limit.
  - `ROUTE_QUEUE_FULL`: overflow policy rejected queued work.
- `CONNECTOR_FAILURE`: connector-level operation error (driver-specific context attached). Because the bucket covers a wide blast radius (timeouts, protocol errors, panics), Chronicle attaches a `reason` label/field (`timeout`, `protocol`, `panic`, or `unknown`) to the corresponding metrics/logs so operators can triage without guessing which sub-class fired.
  - `timeout` captures calls that overrun their configured `timeout`, `warmup_timeout`, or retry-envelope deadlines. This classification is applied before breakers mutate route state so engineers can immediately see which dependency timed out.
  - `protocol` covers well-formed responses from the dependency that still fail policy checks (invalid TLS handshake, auth failures, 4xx/5xx responses that indicate the upstream rejected the request, schema validation failures, etc.).
  - `panic` records executor panics or unexpected runtime exceptions; the label makes it obvious when an internal bug rather than an upstream dependency caused the failure.
  - `unknown` is a catch-all for errors that fail before classification, such as I/O setup faults where the transport layer never surfaces a concrete cause. The loader SHOULD NOT emit `unknown` for steady-state scenarios; seeing this label warrants further instrumentation.
- HTTP triggers translate codes into JSON `{error, route, ts}` payloads with matching HTTP status. Kafka and other async triggers log structured fields `error_code` and `error_detail` and avoid committing offsets.
- Canonical mappings:

| Error code | HTTP status | Async action (Kafka/RabbitMQ/MQTT) | Client guidance |
| --- | --- | --- | --- |
| `TARGET_UNAVAILABLE` | `503 Service Unavailable` + `Retry-After` | Do not commit/ack; pause connector until dependencies recover | Treat as retriable after the advertised `Retry-After` delay. |
| `MISSING_KEY` | `400 Bad Request` | Do not commit; message stays in source for later inspection | Terminal: fix payload or jaq selector; retries will fail deterministically. |
| `BUDGET_EXHAUSTED` | `503 Service Unavailable` | Do not commit; retry once budget refreshes | Retriable only after the effective budget refresh window elapses. |
| `PAYLOAD_TOO_LARGE` | `413 Payload Too Large` | N/A (evaluated only inside HTTP triggers before ingestion) | Terminal until the payload respects configured limits. |
| `ROUTE_QUEUE_FULL` | `429 Too Many Requests` + `Retry-After` | Do not commit; trigger pauses or sheds before acknowledging | Retriable after `Retry-After`; consider slowing send rate. |
| `CONNECTOR_FAILURE` | `502 Bad Gateway` unless overridden by transport | Do not commit; controller evaluates retry budget | Usually transient; adhere to backoff hints before retrying. |

- These mappings decide which HTTP status codes are emitted for overflow (`429` vs `503`) and keep Kafka offsets safe by refusing commits whenever the code indicates that work did not complete deterministically.
- The `Client guidance` column doubles as the retriable/terminal contract SDKs should follow; retry only when explicitly instructed and honour the conveyed `Retry-After` delays.
- Client-side timeouts follow the same taxonomy: idle HTTP uploads hit the listener's deadline and result in `408 Request Timeout` (or `499 Client Closed Request` when the peer disconnects first), while downstream phase expirations return `504 Gateway Timeout`. Both situations surface the internal `CONNECTOR_FAILURE` code because the runtime could not complete the external dependency call. Unhandled panics or unexpected exceptions also translate to HTTP `500`/async `CONNECTOR_FAILURE`, ensuring operators have a single code path to alarm on for "unknown" failures.

### 7.5 Error-State Mapping

| Error code | Readiness / State effect | Notes |
| --- | --- | --- |
| `TARGET_UNAVAILABLE` | Route transitions to `NOT_READY` (or `DEGRADED` when a fallback succeeds) because a required dependency is unhealthy. | Breaker state in `/status.endpoints[]` and metrics such as `chronicle_connector_paused{connector}` immediately reflect the outage. |
| `MISSING_KEY` | No readiness change; treated as a client error. | Chronicle rejects the individual unit of work with `400` (HTTP) or by keeping the message in-source. |
| `BUDGET_EXHAUSTED` | Route transitions to `NOT_READY` until retry budgets refresh. | Exhaustion also increments `chronicle_retry_budget_exhausted_total{route,endpoint}` for on-call correlation. |
| `PAYLOAD_TOO_LARGE` | No readiness change. | Limits enforce ingress hygiene before any route capacity is consumed. |
| `ROUTE_QUEUE_FULL` | Route stays `READY` but the overflow policy dictates the response, and `chronicle_limits_enforced_total` plus `chronicle_route_queue_depth` show sustained pressure. | Operators should raise app-level limits or shed/queue differently if this persists. |
| `CONNECTOR_FAILURE` | Affected endpoint trips its breaker, moving dependent routes to `NOT_READY` until it recovers. | Routes with `allow_partial_delivery: true` may instead enter `DEGRADED` when a fallback handles the failure. |

---

## 8. Performance and Scalability Considerations
- Each chronicle evaluation is a single logical flow, but the engine is built on an async runtime that multiplexes thousands of concurrent phases; connectors should therefore establish pools/clients once at startup and reuse them per call to avoid per-request setup costs.
- jaq expressions are compiled during configuration load; the runtime reuses those compiled plans to avoid per-request parsing and to keep per-phase latency low even under high throughput.
- Backpressure should be managed by the trigger/connector (Kafka consumer offsets, HTTP concurrency limits, etc.); document-level settings like `timeout` help shed work quickly when dependencies stall.
- The configuration is stateless aside from connectors, enabling horizontal scaling by running more Chronicle instances behind load balancers or Kafka consumer groups—operators simply replicate the same config and rely on connector-level partitioning.
- For extremely hot paths, consider batching compatible operations (e.g., Redis pipelines, DB bulk inserts) by introducing additional phase types or options; the design leaves room for such extensions without breaking compatibility.
- Large payloads should stay within `app.limits.http.max_payload_bytes`; Chronicle keeps slot values in memory. Streaming phases remain gated behind feature flags; current behaviour rejects inputs that would breach the limit to prevent uncontrolled memory growth.

---

## 9. Management Section
```yaml
management:
  port: <int>
  live: { path: /live }
  ready: { path: /ready }
  metrics: { path: /metrics }
  status: { path: /status }
```
- `port` is required when management block exists.
- `live` enables the liveness probe endpoint (default `/live`).
- `ready` enables the readiness probe endpoint (default `/ready`).
- `metrics` enables `/metrics` endpoint.
- If both `live` and `ready` are omitted, the management server still starts but only exposes the endpoints that are configured.
- `status` exposes a JSON payload summarising application, route, and endpoint state; defaults to `/status` when present.
- Configuration reloads via HTTP are not supported; restart the process to pick up a new integration file.

### 9.1 Health Endpoints
- **`/live` (Liveness):** Always returns `200 OK` when the process loop is alive. It is independent of downstream health and is safe for Kubernetes liveness probes.
- **`/ready` (Readiness):** Returns `200 OK` only when application policy gates are satisfied (see Section 10). Otherwise responds with `503 Service Unavailable`, `Retry-After`, and a machine-readable body. Results may be cached for up to `app.readiness_cache` to reduce controller churn. Customising the path via `management.ready.path` only changes the HTTP route; the semantics remain the same.
- **Connector hooks:** HTTP connectors must consult readiness on each request (fast-path cache ≤ 250 ms) and immediately return `503` when the associated Route is not ready. Kafka connectors expose route readiness to a controller that pauses/resumes partitions.

### 9.2 Status Payload
`/status` returns detailed JSON for observability and automation:
```json
{
  "app_state": "WARMING_UP|READY|NOT_READY|DEGRADED|DRAINING",
  "routes": [
    {"id":"route-1","state":"READY","requires":["kafka-out"],"unhealthy":[]},
    {"id":"route-2","state":"NOT_READY","requires":["db-main"],"unhealthy":["db-main"]}
  ],
  "endpoints": [
    {"id":"endpoint.db-main","type":"postgres","state":"HEALTHY|UNHEALTHY|CB_OPEN|CB_HALF_OPEN|WARMING_UP"},
    {"id":"endpoint.kafka-out","type":"kafka-producer","state":"HEALTHY"}
  ],
  "ts":"<RFC3339>"
}
```
- `app_state` reflects the current application lifecycle state.
- Each route lists its dependency set and currently unhealthy endpoints.
- Endpoint states mirror circuit breaker and warm-up status.
- Timestamps use RFC3339 format for easy machine parsing.
- Sensitive connector fields (passwords, tokens, private keys) are redacted to `"<redacted>"` before serialization. File paths remain visible for audit.
- Snapshots are atomic: during restarts every `/status` response reflects either the pre-restart or post-restart dependency graph in full, never a mix of partially swapped endpoints.

---

## 10. Runtime Behavior
### 10.1 Operational State Model
- **WARMING_UP:** Application start-up state while connectors and endpoints establish baseline connections and meet warm-up gates.
- **NOT_READY:** Connectors must not accept new work. HTTP listeners respond with `503`, Kafka consumers pause affected partitions.
- **READY:** All required routes and their dependency sets are healthy; connectors admit traffic.
- **DEGRADED:** Optional state when `allow_partial_delivery=true` and fallbacks permit partial target availability. Processing continues with well-defined fallbacks.
- **DRAINING:** Application received a shutdown signal; no new work is accepted, but in-flight work may finish.

State-machine snapshot:
```
          deps healthy
WARMING_UP ---------> READY -- drain signal --> DRAINING
     ^                    |  ^                    |
     | warm-up retry      |  | recovery           | forced exit
     +------ retry/backoff|  |                    v
            when endpoints|  +-- dependency fail -> NOT_READY
                           \
                            +- fallback-only success -> DEGRADED (returns to READY after recovery)
```
See `docs/design/rabbitmq_trigger_state_machine.md` for the lower-level RabbitMQ trigger timing diagram; the same transitions drive every connector.

| From state | Allowed transitions | Trigger |
| --- | --- | --- |
| `WARMING_UP` | `READY`, `NOT_READY`, `DRAINING` | All warm-up gates satisfied; dependency failure during warm-up; shutdown signal |
| `READY` | `NOT_READY`, `DEGRADED`, `DRAINING` | Required dependency unhealthy; optional dependency failure with fallback; shutdown signal |
| `NOT_READY` | `READY`, `DRAINING` | All required dependencies healthy; shutdown signal |
| `DEGRADED` | `READY`, `NOT_READY`, `DRAINING` | Optional dependency recovers; fallback fails and dependency becomes required again; shutdown signal |
| `DRAINING` | (terminal) | Process exits after `app.drain_timeout` (+5 s hard stop) |

### 10.2 State Transitions
- **Startup:** Application enters `WARMING_UP` immediately. Endpoints flagged `warmup: true` establish connections, pools, and metadata before readiness probes succeed. When all warm-up and endpoint health checks pass, Chronicle transitions to `READY` (or `NOT_READY` if dependencies remain unhealthy).
- **Runtime:** `READY → NOT_READY` occurs when any Route's dependency health fails. Recovery triggers `NOT_READY → READY` once all required endpoints report `HEALTHY` or `CB_HALF_OPEN` with successful probes. `READY → DRAINING` occurs when graceful shutdown is initiated; once drains complete the process exits.
- **Monotonic behaviour:** Failures drive state transitions pessimistically (downwards) while recovery is optimistic but requires verification through health probes. Warm-up does not revert once completed.

### 10.3 Policy Gates
- **Startup Gates:**
  - `app.min_ready_routes` controls how many Routes must reach `READY` before the application reports overall readiness. Accepted values: positive integer or `all`. When numeric, routes are evaluated in ascending `policy.readiness_priority` order; ties fall back to configuration order to keep decisions deterministic.
  - Each Route evaluates the union of inferred dependencies plus `policy.requires`. All endpoints with `warmup: true` must complete connection establishment before readiness is evaluated.
  - Database connectors honour `pool.min_connections`. Falling below the minimum during warm-up or runtime marks the endpoint `UNHEALTHY`.
- **Runtime Gates:**
  - If any Endpoint in a Route’s Dependency Set is `UNHEALTHY` or `CB_OPEN`, the Route transitions to `NOT_READY`. `CB_HALF_OPEN` counts as ready only when the relevant `half_open_counts_as_ready` policy allows it.
  - When `half_open_counts_as_ready = never` (globally or per route), half-open probes continue to run but readiness does not flip back to `READY` until a probe succeeds and closes the breaker; probes alone never grant readiness credit.
  - Application readiness is computed as the conjunction/threshold defined by `min_ready_routes`.
- Readiness propagation delay is bounded by `app.readiness_cache` plus one controller tick (≤10 ms). In practice, once an endpoint recovers, the worst-case time before `/ready` reflects the change is `readiness_cache` (default 250 ms). Operators should size the cache with this guarantee in mind.
- **Backpressure & Pausing:**
  - HTTP connectors return `503` with `Retry-After: <seconds>` and `Cache-Control: no-store` whenever the route is not ready or warming up. When only concurrency limits are exceeded, connectors emit `429 Too Many Requests` with the retry budget's next delay.
  - Kafka consumers pause specific topic/partition assignments on readiness failure and resume automatically upon recovery.
  - Per-route overflow policy decides whether surplus work queues (bounded), rejects, or is shed. Queueing honours `max_queue_depth` and emits metrics when nearing saturation.
- **Delivery Semantics:**
  - Default delivery is at-least-once. Endpoints should enable idempotent publisher features where available (`idempotent: true`, Kafka idempotent producers, DB transactions).
  - Retries respect the route/phase `delivery` policy and interact with circuit breakers to avoid infinite retry loops. Retry attempts that would exceed the effective budget are skipped and surfaced as `BUDGET_EXHAUSTED` errors.
- **Failure Visibility & Isolation:**
  - Partial delivery attempts are prohibited unless `allow_partial_delivery=true` and explicit `fallback` logic is provided.
  - Routes that rely on fallbacks transition to `DEGRADED` to signal reduced capacity while continuing to process work.
  - Circuit breaker state is isolated per Endpoint; unrelated Routes remain READY unless the global readiness threshold requires them.
- **Warm-Up Timing Policy:**
  - Each warm-up-enabled Endpoint uses `warmup_timeout` (default 30 s). Failure to initialise within the window marks it `UNHEALTHY`. Background warm-up retries continue until success or shutdown.
  - No Connector begins accepting traffic before warm-up completes, regardless of listener readiness.

### 10.4 Execution Flow
1. Load YAML configuration into structured models (`app`, `connectors`, `chronicles`, `management`).
2. Validate connector uniqueness, chronicle integrity, and route dependency graphs.
3. Instantiate connector instances (Kafka clients, HTTP servers/clients, DB pools) and run warm-up tasks for endpoints flagged `warmup: true`.
4. Register routes with the readiness controller, wiring dependency sets, circuit breakers, and delivery policy.
5. Bind triggers to connectors dynamically.
6. On trigger activation:
   - Consult readiness cache; reject early if the route is not ready.
   - Capture the inbound payload as structured JSON in slot `.[0]`.
   - Execute each phase in order:
     - Resolve required jaq expressions.
     - Execute the phase operation (pure transforms first, endpoint interactions last).
     - Persist the resulting JSON into the next slot.
   - On success, return or commit result.
   - On failure, apply delivery policy (retries, breaker accounting) and record telemetry before aborting or retrying as configured.

### 10.5 Graceful Shutdown
- Enter `DRAINING` upon receiving termination signals.
- HTTP connectors stop accepting new requests and respond with `503`/`Retry-After`; in-flight requests finish normally. Long-running requests inherit a shutdown deadline (`app.drain_timeout`, default `30s`) after which they are cancelled with `499 Client Closed Request`.
- Kafka consumers stop polling, finish current in-flight messages up to the configured limit, and commit offsets only after successful completion. Messages exceeding the drain deadline are nacked and re-queued.
- Database transactions respect the same deadline: Chronicle sends a driver-level cancel (Postgres `pg_cancel_backend`, MariaDB `KILL QUERY`, etc.) when `drain_timeout` elapses. Retrying those operations after the cancel is prohibited while the application is in `DRAINING`; once shutdown completes, any uncommitted work is rolled back by the database.
- Once all routes finish draining or the drain deadline elapses, the process exits. A final `SIGKILL` is sent by orchestration five seconds after the drain window to ensure termination.


---

## 11. Observability
Chronicle exposes health, metrics, logs, and traces so operators can diagnose warm-up, readiness, and delivery issues.

### 11.1 Metrics
- Prometheus-style metrics exported via `/metrics` when enabled:
All Chronicle-specific metrics emitted via `/metrics` share the `chronicle_` prefix, including:
  - `chronicle_app_state{}` gauge: 0=`WARMING_UP`, 1=`NOT_READY`, 2=`READY`, 3=`DEGRADED`, 4=`DRAINING`.
  - `chronicle_route_state{route=}` gauge for per-route lifecycle.
  - `chronicle_endpoint_state{endpoint=}` gauge: 0=`WARMING_UP`, 1=`UNHEALTHY`, 2=`HEALTHY`, 3=`CB_OPEN`, 4=`CB_HALF_OPEN`.
  - `chronicle_connector_paused{connector=}` gauge surfaces Kafka/HTTP backpressure decisions.
  - `warmup_duration_seconds{endpoint=}` summary to measure warm-up completion time.
  - `http_requests_total{code,route}` counter, latency histograms per route.
  - `kafka_poll_pauses_total{route}` counter for controller pause events.
  - `cb_open_total{endpoint}` and `cb_half_open_total{endpoint}` counters.
  - Endpoint-specific latency/timeout histograms (HTTP client, Kafka produce, DB operations).
  - `chronicle_route_queue_depth{route}` gauge exposes instantaneous queue utilisation when `overflow_policy=queue`.
  - `chronicle_shed_total{route}` counter separates shed events from rejections so SLOs stay unambiguous.
- `chronicle_limits_enforced_total{route,policy}` counter for limit hits (reject/queue/shed) and `chronicle_retry_budget_exhausted_total{route,endpoint}` counter for budget exhaustion.
- All metrics or structured events that surface `error_code="CONNECTOR_FAILURE"` also export `reason` labels populated with the taxonomy described in §7.4 (`timeout`, `protocol`, `panic`, `unknown`). Alerting rules SHOULD include the label so triage dashboards can split along the dominant cause without adding bespoke error codes.
- `chronicle_retry_attempts_total{route,endpoint,reason}` counts actual retry attempts, differentiating warm-up probes, half-open probes, and steady-state delivery.
- `chronicle_half_open_probe_concurrency{endpoint}` gauge reports how many concurrent probes are active per endpoint while breakers are `CB_HALF_OPEN`.
- `chronicle_parallel_children_total{route,phase,status}` tracks the final outcome of each child inside parallel phases without exposing per-child names as labels.
- Histogram series currently omit exemplars/span links; exporters MAY add them in the future once tracing IDs are stable.
- Example emit for the HTTP → Kafka sample route:
  ```text
  chronicle_route_state{route="collect_record"} 2
  chronicle_retry_budget_exhausted_total{route="collect_record",endpoint="endpoint.sample_kafka_cluster"} 0
  chronicle_parallel_children_total{route="collect_record",phase="fanout",status="success"} 42
  ```
- Label cardinality is bounded: route and endpoint labels are truncated to 40 characters, and only whitelisted label keys (`route`, `endpoint`, `status`, `code`) are exposed. Histograms share static bucket definitions per transport to keep scrape cost predictable.
- Endpoint labels map one-to-one with connectors (Section 1.1). If multiple routes reuse the same connector, they share the same endpoint label, which keeps breaker telemetry centralized.
- All duration-bearing series terminate in `_seconds` to follow Prometheus conventions; consumers should not expect millisecond-scale units.

### 11.2 Logs
- Structured logs include `route`, `endpoint`, `state_from`, `state_to`, `reason`, `correlation_id`, and `trace_id`.
- Warm-up events, circuit breaker transitions, retries, and readiness changes emit explicit log entries for audit.
- Connector components should log pause/resume actions with partition/topic details to ease Kafka debugging.
- `/status.ts` and log timestamps are both emitted in UTC using the same monotonic-backed system clock, so correlating readiness events with log lines requires no skew adjustment beyond the ±2 s tolerance described in Section 1.7.

### 11.3 Tracing
- Spans cover the entire flow from connector receive → transforms → endpoint interactions.
- Circuit breaker and warm-up details are tagged on spans (`cb_state`, `warmup=true/false`).
- Trace contexts adhere to the W3C `traceparent`/`tracestate` format. HTTP triggers read and forward these headers; Kafka, RabbitMQ, and MQTT transports map them to message headers/properties so downstream systems can stitch timelines together.

### 11.4 Diagnostics Commands
- Operators can use the `/status` payload plus `/ready` and `/live` to correlate readiness failures with endpoint health.
- Recommended CLI helpers for runbooks:
  - `curl -sf localhost:9090/ready`
  - `curl -s localhost:9090/status | jq`
  - `curl -s localhost:9090/metrics | grep app_state`

---

## 12. Implementation Checklist

1. **Configuration model**: support dynamic maps, dotted keys, and new `app`/route policy fields.
2. **YAML loader**: preserve key case and ordering.
3. **Context evaluator**: jaq runtime that evaluates expressions against native JSON slots.
4. **Executor registry**: map phase types to handler functions.
5. **Trigger framework**: instantiate HTTP/Kafka listeners dynamically with readiness hooks.
6. **Option interpolator**: resolve jaq expressions with fatal-on-missing semantics.
7. **Connector providers**: implement runtime factories, warm-up scheduling, and dependency registration.
8. **Limits & flow control**: enforce app/route concurrency ceilings, overflow policies, payload limits, and expose enforcement metrics.
9. **Circuit breaker & retry layer**: enforce per-endpoint retry budgets, breaker transitions, and background recovery loops that respect half-open policy.
10. **Readiness controller**: aggregate route states, manage `min_ready_routes`, drive HTTP 503s/Kafka pauses, and integrate DEGRADED transitions.
11. **Phase adapters**: implement core executors, idempotency hints, and respect delivery policy.
12. **Observability**: structured logs, metrics, traces, `/status` payload with redaction and bounded cardinality.
13. **Integration tests**: validate warm-up gating, readiness transitions, chaos cases (broker flaps, TLS expiry, slowloris HTTP), and jaq/dependency inference fixtures.
14. **Validation CLI**: keep the `chronicle validate` command wired to the runtime loader so CI/lint hooks enforce the same schema (role inference, limit inequalities, feature flags) that production binaries expect.
15. **Constraint fixtures**: add regression YAMLs covering cross-scope constraints (budget intersections, `policy.limits ≤ app.limits`, kafka idempotence requirements) so schema or code changes cannot loosen guarantees silently.

### 12.1 Runtime isolation and thread-safety
- Chronicle runs on Tokio; every connector handle must be `Send + Sync` so routes can share pools across worker tasks without cloning transports per request.
- Endpoints are reference-counted (`Arc`) and guarded by per-endpoint breakers; never mutate connector options after activation because multiple routes may observe the same handle concurrently.
- Trigger listeners (HTTP, Kafka, AMQP) run on dedicated tasks but schedule phase execution back onto the shared runtime. Use bounded channels between trigger and executor paths so pauses or readiness changes cannot deadlock.
- When integrating new transports, ensure driver callbacks never block the runtime (offload to blocking pools when necessary) and expose cancellation hooks so `app.drain_timeout` can interrupt long-running I/O cleanly.

---

## 13. Example Chronicle Execution

**collect_record**
- Trigger: HTTP POST `/api/v1/records`.
- Phase 1: Transform the nested `record` payload into a summarized structure.
- Phase 2: Publish the record plus trace metadata to Kafka.
- Phase 3: Return a 200 response describing downstream fan-out status.

**relay_record**
- Trigger: Kafka consumer on `records.samples`.
- Phase 1: HTTP POST to forward the record summary to another system.

**persist_record**
- Trigger: HTTP POST `/api/v1/records`.
- Phase 1: Insert the record attributes/metrics into MariaDB.
- Phase 2: Respond with confirmation JSON including the inserted identifier.

---

## 14. Acceptance Criteria
1. **Startup (Warm-Up):** Endpoints with `warmup: true` attempt immediate connection establishment on process start. The application remains in `WARMING_UP` until all such endpoints report `HEALTHY`, and `/ready` returns `503` with state details during this phase.
2. **Scenario 1 (HTTP → Kafka):** With Kafka producers unavailable, HTTP handlers reject requests within 50 ms using `503 Service Unavailable`, `Retry-After`, and body `{"error":"TARGET_UNAVAILABLE","route":"<id>","ts":"<rfc3339>"}`. `/ready` reflects recovery within one probe interval once Kafka is healthy.
3. **Scenario 2 (Kafka → DB):** When the database pool falls below `min_connections`, the relevant Kafka consumer pauses partitions within one control cycle. No new messages are fetched until the DB recovers and readiness returns to `READY`.
4. **Recovery:** Endpoints attempt reconnection with exponential backoff and jitter. Successful probes transition circuit breakers Half-Open → Closed, restore route readiness, and resume Kafka consumption or HTTP acceptance.
5. **Isolation:** Failures in one Route's Endpoint do not block unrelated Routes unless required by `app.min_ready_routes`. Circuit breaker state is per endpoint instance.
6. **Observability:** Metrics, logs, and `/status` entries exist for every state transition, circuit breaker change, and warm-up event to support troubleshooting.

---

## Appendix A. Metrics Histogram Buckets

| Transport / Phase | Metric Prefix | Buckets (seconds) |
| --- | --- | --- |
| HTTP client & HTTP trigger handlers | `http_request_duration_seconds`, `http_trigger_duration_seconds` | 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10 |
| gRPC client | `grpc_client_duration_seconds` | 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5 |
| Kafka produce & consume | `kafka_phase_duration_seconds`, `kafka_poll_duration_seconds` | 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1 |
| Database phases (Postgres, MariaDB, MongoDB) | `db_phase_duration_seconds` | 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5 |
| Cache & queue clients (Redis, RabbitMQ, MQTT) | `cache_phase_duration_seconds` | 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5 |

Buckets are immutable per transport to avoid scrape churn. Custom transports must either reuse one of the tables above or contribute an addition to this appendix before merging.

## Appendix B. Field-path Index

| Path | Description | Section |
| --- | --- | --- |
| `app.retry_budget.*` | Global retry defaults (attempts, elapsed, backoff, jitter). | §1.5 |
| `app.limits.routes.*` | Application-wide concurrency/overflow defaults. | §1.4 |
| `app.feature_flags[]` | Enables connector/phase families guarded by feature gates. | §1.3 |
| `connectors[].warmup`, `.cb.*`, `.limits.*`, `.retry_budget.*` | Cross-cutting connector knobs applied per endpoint. | §2.1 |
| `chronicles[].policy.delivery.*` | Route-level delivery policy inherited by outbound phases. | §3.3 |
| `chronicles[].policy.retry_budget.*` | Route-specific retry intersection overrides. | §3.3 |
| `chronicles[].policy.requires[]` | Explicit dependency set additions beyond inferred endpoints. | §3.3 |
| `phases[].timeout` | Per-phase execution bound for outbound work. | §4.1 |
| `phases[].options.connector` | Reference to connector/endpoint used by the phase. | §5.* |
| `management.ready.path`, `.status.path` | Custom management endpoint routing. | §9 |
