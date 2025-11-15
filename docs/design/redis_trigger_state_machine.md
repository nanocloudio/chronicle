# Redis Trigger Runtime State Machine

## Goals
- Cover both Redis trigger modes from `docs/specification.md` §3.2 (pub/sub and streams) with consistent payload shapes and backpressure counters.
- Encapsulate connector nuances (TLS, auth, consumer groups) behind a `RedisTriggerDriver` abstraction that can reconnect without restarting the runtime.
- Provide at-least-once semantics: stream entries are ACKed only after successful chronicle execution, while pub/sub deliveries remain fire-and-forget.

## States & Transitions

```text
┌──────────────┐  driver_ok  ┌──────────────┐  delivery  ┌──────────────┐  exec_ok  ┌────────────┐   ack_ok   ┌────────────┐
│ Initializing │────────────▶│  Receiving   │───────────▶│   Executing   │─────────▶│ Completed  │──────────▶│    Idle    │
└──────┬───────┘             └──────┬───────┘            └──────┬───────┘          └──────┬────┘          └──────┬─────┘
       │ build_err                  │ recv_err                 │ exec_err               │ ack_err              │ idle_tick
       ▼                            ▼                          ▼                        ▼                     ▼
┌──────────────┐       abort?     ┌──────────────┐    backoff    ┌──────────────┐  log+drop  ┌──────────────┐ log+retry ┌────────────┐
│  BuildFailed │─────────────────▶│ Reconnecting │◀──────────────│   Backoff    │◀──────────▶│  Completed   │◀─────────▶│    Idle    │
└──────────────┘   shutdown       └──────────────┘               └──────────────┘            └──────────────┘           └────────────┘
```

### State Descriptions
- **Initializing** — Parse trigger `options` (`mode`, `channels`, `stream`, `group`), merge retry settings, and build a `RedisTriggerDriver` (pub/sub subscription or XREAD loop). Any misconfiguration raises `RedisTriggerError::*`.
- **Receiving** — The driver’s `next_delivery()` yields `RedisDelivery::PubSub` or `RedisDelivery::Stream`. Idle polls call `idle_with_backoff` to reduce CPU usage.
- **Executing** — Build the JSON payload:
  - Pub/Sub: `channel`, binary payload (base64 + optional decoded text), empty `attributes`, timestamp metadata.
  - Stream: `stream`, entry `id`, field map split between `payload` (if `data` exists) and `attributes`, metadata with timestamp + `stream_id`.
  Invoke `ChronicleEngine::execute` with runtime counters tracking inflight work.
- **Completed** — Upon success, ACK stream entries via `driver.ack`. Pub/Sub deliveries skip ACK because the broker already fan-outs.
- **Idle** — No deliveries available; `idle_with_backoff` resets the exponential backoff success path and sleeps for ~50 ms.
- **Reconnecting** — Triggered when `next_delivery()` or `ack()` returns an error. The runtime logs the failure, invokes `driver.reconnect()`, and applies the current backoff delay before resuming `Receiving`.

## Delivery Lifecycle
1. `RedisTriggerConfig` binds the chronicle, connector (URL, TLS, auth), trigger options, and merged `RetrySettings`.
2. `RedisDriver` establishes either:
   - **Pub/Sub**: a `PubSub` subscription that streams messages directly.
   - **Stream**: a `ConnectionManager` issuing blocking `XREAD` calls (grouped if `group` exists, else simple `last_id` polling). Consumer names include the chronicle and a UUID.
3. For each delivery:
   - Increment `runtime_counters().redis_trigger_inflight`.
   - Build the JSON payload via `build_trigger_payload`.
   - Execute the chronicle; errors are logged but do not ACK the stream entry (so Redis retains it).
   - ACK stream entries via `XACK` when `group` is configured; otherwise skip.
   - Decrement inflight counters.

## Error & Retry Semantics
- Connector lookups, TLS setup, and subscription/stream creation errors are surfaced as `RedisTriggerError` variants that halt startup.
- Runtime receive errors enter `error_with_backoff`, which logs the incident, attempts `driver.reconnect()`, and sleeps for the computed delay.
- ACK failures log detailed context (stream, group, id) but keep the runtime running so operators can correct Redis state manually.
- In stream mode without consumer groups, the driver tracks `last_id` to guarantee new entries continue even after reconnects.
- Shutdown tokens cancel the `broker_loop!`, but the runtime waits for any in-flight executions before exiting.

## Telemetry
- Logs under `chronicle::redis` capture driver receive failures, reconnect attempts, payload build issues, trigger completion/failure, and ack errors.
- Inflight counters expose Redis trigger load to readiness/observability tools.
- Mode, stream/channel, and chronicle names appear in every log entry for contextual debugging.

## Integration Points
- Uses `ConnectorRegistry::redis` for URL/auth/TLS plus connector extras for retry defaults.
- The pluggable `RedisTriggerDriver` makes it easy to inject fake drivers in tests or extend with cluster/resp3 support later.
- Transport identity is `TransportKind::RedisIn`, so the runtime participates in shared health checks and shutdown orchestration.

## Open Questions
- Should we fan out stream consumers per chronicle to increase throughput, or rely on Redis consumer groups instead?
- Do we need dead-letter handling for malformed stream entries (e.g., repeated `payload_build_failed`) rather than just logging?
