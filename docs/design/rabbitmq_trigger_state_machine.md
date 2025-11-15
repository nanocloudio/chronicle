# RabbitMQ Trigger Runtime State Machine

## Goals
- Honour the specification requirements in `docs/specification.md` (§3.2 RabbitMQ Trigger) covering queue consumption, ack semantics, per-consumer prefetch, and redelivery handling.
- Provide clear checkpoints for resilience features (connection retries, drain on shutdown) and observability (metrics/logs per state transition).
- Integrate with Chronicle's backpressure semaphores so the trigger respects engine-wide concurrency limits.

## States & Transitions

```text
┌──────────────┐   open(channel)   ┌────────────────┐
│  Connecting  │──────────────────▶│  DeclaringQoS  │
└──────┬───────┘                   └───────┬────────┘
       │ on_failure                        │ qos_ok
       ▼                                   ▼
┌──────────────┐  retry_delay  ┌────────────────┐  consume_ok  ┌──────────────┐
│ Reconnecting │◀──────────────│   Consuming    │─────────────▶│  Delivering  │
└──────┬───────┘               └───────┬────────┘              └──────┬───────┘
       │ backoff_expired               │ cancel/close                 │ idle
       ▼                               ▼                             ▼
┌──────────────┐        cancel_ok     ┌────────────────┐  all_acked  ┌────────────────┐
│ ShuttingDown │─────────────────────▶│  DrainingQueue │────────────▶│  Disconnected  │
└──────────────┘                      └────────────────┘             └────────────────┘
```

### State Descriptions
- **Connecting** — create connection + channel using connector config. Emits telemetry for host/virtual host. Failures transition to `Reconnecting`.
- **DeclaringQoS** — apply `basic_qos(prefetch)` derived from trigger `options.prefetch` (or connector default). Errors return to `Reconnecting`.
- **Consuming** — issue `basic_consume`. On success transition to `Delivering`; on `basic_cancel_ok` transition to `DrainingQueue`.
- **Delivering** — messages flow into Chronicle engine. Each delivery acquires the engine's backpressure permit before spawning the chronicle execution. The delivery holds a handle so that completion code calls `basic_ack`/`basic_nack`.
- **Reconnecting** — waits with exponential backoff (`prefetch` unaffected) before re-entering `Connecting`. Tracks attempt counters for observability.
- **ShuttingDown** — triggered by cancellation token. Cancels the consumer and waits for in-flight deliveries to settle.
- **DrainingQueue** — once `basic_cancel_ok` received, continue servicing deliveries until acked (bounded by backpressure). After queue is empty and all acks processed transition to `Disconnected`.
- **Disconnected** — terminal state; resources cleaned up. Allows restart cycle if runtime restarts trigger subsystem.

## Delivery Lifecycle
1. Delivery arrives (`Delivering`).
2. Acquire backpressure permit (`Backpressure::acquire(TriggerKind::Rabbitmq)`).
3. Wrap payload into trigger slot JSON:
   ```json
   {
     "body": <base64 payload or decoded JSON>,
     "headers": {...},
     "properties": {...},
     "routing_key": "...",
     "exchange": "...",
     "delivery_tag": 123,
     "redelivered": false
   }
   ```
4. Execute chronicle pipeline. On success call `basic_ack(delivery_tag)`. On failure apply trigger-level policy: `ack_mode = auto` ⇒ always `basic_ack`; `ack_mode = manual` ⇒ `basic_nack` with `requeue=true` unless phase opted out (future option).
5. Release backpressure permit.

## Prefetch Handling
- QoS source order: trigger `options.prefetch` → connector `prefetch` → broker default.
- Prefetch applied once per channel (state `DeclaringQoS`).
- When backpressure queue is full the consumer is paused (`basic_nack` with requeue=false` + internal buffer) until capacity returns.

## Error & Retry Semantics
- Connection failures record attempt metrics and sleep using capped exponential backoff (e.g., 250ms → 2s → 5s → 10s).
- AMQP channel closures while consuming re-enter `Reconnecting`.
- If `basic_ack` fails (channel closed) the delivery transitions to `Reconnecting` after logging error and increments a `ack_retry` counter for observability.

## Telemetry
- Emit structured logs/metrics on:
  - State transitions (e.g., `rabbitmq_trigger.state_change` with `from`/`to`).
  - Delivered messages (`delivery_tag`, `routing_key`, `exchange`, `acked`, `latency_ms`).
  - Reconnect attempts + duration.
- Integrate with existing `ChronicleAction` metrics by annotating trigger name and connector.

## Integration Points
- Uses `ConnectorFactoryRegistry::rabbitmq` handle to access URL, TLS paths, and prefetch defaults.
- Test coverage uses the newly added `MockRabbitmqBroker/Client` to simulate deliveries, ack cycles, and reconnect triggers.
- The state machine will live under `src/chronicle/rabbitmq_triggers.rs` with an async `run` loop (Tokio tasks) registered alongside HTTP/Kafka triggers in `ChronicleApp`.

## Open Questions
- Should manual ack failures escalate to saga compensations or simply nack? (default: nack with requeue).
- How do we expose dead-letter routing decisions (maybe via future trigger options).
- Metrics naming alignment with existing Kafka trigger instrumentation.
