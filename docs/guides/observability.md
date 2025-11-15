# Chronicle Observability Guide

This document consolidates all runtime telemetry guidance: management endpoints,
metrics, dashboards, alerts, and log search workflows. Pair it with
`docs/guides/operations.md` for lifecycle procedures and `docs/guides/security.md` for hardening.

## 1. Management Endpoints
- **`/live`** (`management.live.path`) returns `200 OK` whenever the process loop
  is alive. Use it for liveness probes; payload includes connector inventory but
  never blocks traffic.
- **`/ready`** (`management.ready.path`) reflects the readiness controller. A
  healthy app returns `200 OK` plus route summaries; unhealthy states return
  `503` with the failing routes and set `Retry-After` to the configured cache
  window. Responses are cached for up to `app.readiness_cache` milliseconds and
  invalidated immediately when dependencies change.
- **`/status`** (`management.status.path`) surfaces `app_state`, per-route
  dependencies/unhealthy sets, endpoint health, and an RFC3339 timestamp.
- **`/metrics`** emits Prometheus text, including readiness gauges
  (`chronicle_app_state`, `chronicle_route_state`, `chronicle_endpoint_state`)
  plus connector/backpressure counters.
- Custom paths are normalized (missing `/` prefixes fixed automatically). Secure
  the management surface per `docs/guides/security.md`.

Chronicle collects the metrics described below through the `chronicle::metrics`
facade so the same counter families appear in transports, the management endpoint,
and regression tests. Backpressure snapshots (`chronicle_backpressure_http_*`,
`chronicle_backpressure_kafka_*`) reflect the `BackpressureController` state, and
connector actions (`chronicle_connector_actions_total`) reuse the helper that each
phase and readiness transition calls.

## 2. Prometheus Metrics & Dashboards
Use these starter snippets (YAML or JSON equivalents) to build Grafana panels:

### Throughput & Error Rates
```yaml
title: Chronicle Throughput
targets:
  - expr: rate(chronicle_actions_total[5m])
    legend: '{{chronicle}} success'
  - expr: rate(chronicle_action_errors_total[5m])
    legend: '{{chronicle}} errors'
description: Overlay completed vs failed chronicles.
```

### Backpressure Heatmaps
```yaml
title: HTTP Backpressure
graphs:
  - expr: chronicle_backpressure_http_inflight
    legend: inflight
  - expr: chronicle_backpressure_http_throttled_total
    legend: throttled_total
  - expr: chronicle_backpressure_http_max_concurrency
    legend: max_concurrency
description: Compare configured permits to actual usage.
```

### Kafka Consumer Health
```yaml
title: Kafka Backpressure
targets:
  - expr: chronicle_backpressure_kafka_inflight
  - expr: chronicle_backpressure_kafka_throttled_total
  - expr: chronicle_kafka_connectors_present
description: Throttled deltas serve as a lag proxy; pair with broker metrics.
```

### Connector Inventory
```yaml
title: Connector Inventory
annotations:
  - expr: chronicle_connectors_total
graphs:
  - expr: chronicle_database_configured
  - expr: chronicle_kafka_connectors_present
description: Sanity check for expected connector types at startup.
```

## 3. Alert Catalog
| Alert | Expression | Threshold | Rationale |
| --- | --- | --- | --- |
| `ChronicleHighHTTPThrottling` | `increase(chronicle_backpressure_http_throttled_total[10m])` | >500 /10m | HTTP ingress saturation; scale replicas or widen permits. |
| `ChronicleKafkaDrainingSlow` | `increase(chronicle_backpressure_kafka_throttled_total[5m])` | >100 /5m | Kafka consumers blocked by downstream dependencies. |
| `ChronicleErrorRatio` | `rate(chronicle_action_errors_total[5m]) / rate(chronicle_actions_total[5m])` | >0.02 | Chronicle-level failure budget. Use `trace_id` to diagnose. |
| `ChronicleConfigDrift` | `chronicle_connectors_total{type="none"} != 0` | n/a | Signals invalid config/no connectors loaded. |

## 4. Logging & Tracing
- Logs are structured key/value lines: `ts`, `level`, `service`, `component`,
  `msg`, plus context. Values with spaces are quoted for log routers.
- Availability transitions add `route`, `endpoint`, `state_from`, `state_to`,
  and `reason`. Warm-up events include `duration_ms`/`attempts`.
- Trace correlation rides on `trace_id` / `correlation_id` from triggers while the canonical `record_id`
  and `route` metadata remain accessible from `ExecutionContext`. Logs, metrics, and status payloads
  reference these names so alerts and dashboards can join insights without having to guess which slot
  the values live in.
- Manual readiness overrides log `reason=external_set_route_state`; automated
  updates use `reason=dependency_update`.

### Log Search Recipes
- **Trace stitching**: `trace_id="<value>"` gathers every chronicle/phase log.
- **Connector failures**: filter on `component="chronicle::engine"` and
  `event="connector_action_failed"` plus `connector="<name>"`.
- **Backpressure spikes**: `component="chronicle::engine" event="chronicle_completed"`
  sorted by `actions` to find large fan-outs before throttling triggers.
- **Readiness flaps**: `reason="dependency_update"` grouped by `route` or
  `endpoint`.

## 5. Implementation Checklist
1. Ensure `management.metrics` is enabled so `/metrics` emits counters above.
2. Import the provided snippets into Grafana (or convert to JSON dashboards).
3. Configure Alertmanager/PagerDuty/Stackdriver rules with the alert catalog.
4. Share standardized log searches with on-call staff so alerts map to traces
   quickly.

Keep this file authoritative—delete ad-hoc observability docs rather than
diverging from a single source.***
