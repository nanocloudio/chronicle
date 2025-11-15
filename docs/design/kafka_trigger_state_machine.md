# Kafka Trigger Runtime State Machine

## Goals
- Implement `docs/specification.md` §3.2 Kafka Trigger guarantees around topic/subscription wiring, consumer group coordination, and payload shape.
- Provide clear sequencing for polling, executing, and committing offsets so operators know when records are acked vs. reprocessed.
- Surface transient broker failures separately from application/chronicle errors to speed recovery playbooks.

## States & Transitions

```text
┌──────────────┐  build_ok  ┌─────────────┐  sub_ok  ┌─────────────┐   record   ┌──────────────┐  commit_ok  ┌──────────────┐
│ Initializing │───────────▶│ Subscribing │─────────▶│   Polling    │──────────▶│  Executing   │────────────▶│  Committing  │
└──────┬───────┘            └──────┬──────┘         └──────┬───────┘            └──────┬──────┘              └──────┬───────┘
       │ build_err                 │ sub_err               │ poll_err                │ exec_err                  │ commit_err
       ▼                           ▼                       ▼                         ▼                           ▼
┌──────────────┐        retry     ┌──────────────┐      backoff      ┌──────────────┐   log+drop    ┌────────────┐ log+retry ┌────────────┐
│  BuildFailed │─────────────────▶│ Reconnecting │◀──────────────────│   Backoff    │◀─────────────▶│  Executing  │◀─────────▶│   Error    │
└──────────────┘   shutdown        └──────────────┘                   └──────────────┘              └────────────┘            └────────────┘
```

### State Descriptions
- **Initializing** — Iterate chronicles, derive `KafkaTriggerOptions`, construct a `KafkaConsumerConfig`, and create the transport-specific consumer implementation.
- **Subscribing** — Invoke `KafkaConsumer::subscribe` for the single topic per chronicle. Failures go to `Reconnecting` with error metrics.
- **Polling** — The `broker_loop!` macro repeatedly calls `poll()`. Empty polls go through `Backoff` (50 ms) to avoid busy loops.
- **Executing** — Convert the `KafkaRecord` (topic/partition/offset, headers, payload/key) into slot `.[0]` and execute the chronicle. Errors log but the record is not committed so Kafka redelivers based on the consumer group settings.
- **Committing** — After a successful execution, call `KafkaConsumer::commit` with logging wrappers. Commit failures log errors yet stay in `Polling`; the record may repeat, which is acceptable per at-least-once semantics.
- **Reconnecting** — Triggered when `poll()` returns an error or subscription fails. Uses incremental backoff via `wait_backoff` (50 ms → 1 s) before retrying subscriptions/polls.
- **Backoff** — Idle state inside the loop used for both polite polling gaps and error recovery delays.

## Delivery Lifecycle
1. Subscribe to the configured topic and consumer group.
2. `poll()` yields a record or `None`; `None` results lead to a short idle sleep.
3. Build the trigger payload:
   - `topic`, `partition`, `offset`, `timestamp`.
   - `headers` map (binary values are base64-encoded when not valid UTF-8).
   - `key` and `value` each include both `base64` and optional decoded text/JSON representations via `parse_binary`.
4. Execute the chronicle; if the engine errors, the log entry includes `offset` so operators can correlate with broker tooling.
5. `commit_with_logging` acknowledges the offset; repeated failures still keep the runtime alive but cause replays.

## Error & Retry Semantics
- Consumer construction/subscription failures bubble up as `KafkaTriggerError::*` so misconfigurations abort fast.
- Poll errors enter `Reconnecting` with exponential-ish backoff and structured telemetry (`consumer_poll_failed`).
- Chronicle execution failures do not kill the transport; Kafka keeps the record in the partition until a later successful commit.
- Commits are retried once inside `commit_with_logging`; repeated issues are only logged to avoid livelocks.
- Shutdown signals propagate through `TaskTransportRuntime`, cancelling each spawned consumer task gracefully.

## Telemetry
- Trace logs for every received/completed record include chronicle name, topic, and offset for auditing.
- Dedicated events (`offset_commit_failed`, `consumer_poll_failed`) help differentiate broker connectivity vs. business logic issues.
- Consumer counts are exposed via `KafkaTriggerRuntime::consumer_count()` for readiness endpoints.

## Integration Points
- Uses `ConnectorRegistry::kafka` for broker list, TLS/auth, and connector extras.
- Relies on the shared `ChronicleEngine` but bypasses the HTTP dispatcher because Kafka triggers have no response channel.
- The `KafkaConsumer` trait lets tests inject fakes; production uses rdkafka-based implementations delivered elsewhere.

## Open Questions
- Should we add circuit-breaking when repeated `consumer_poll_failed` errors exceed a threshold to avoid log spam?
- Is there demand for per-record transforms (e.g., jaq filters) beyond what the specification hints at in `options.transform`?
