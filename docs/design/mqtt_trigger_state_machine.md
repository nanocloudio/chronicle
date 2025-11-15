# MQTT Trigger Runtime State Machine

## Goals
- Satisfy `docs/specification.md` §3.2 MQTT Trigger guarantees: topic subscription, QoS-aware acknowledgements, retained-message handling, and payload structure.
- Provide predictable reconnect/backoff behaviour so brokers that drop idle clients do not cause load spikes.
- Keep runtimes observable: expose when packets are skipped (retained) vs. executed, and when QoS acknowledgements fail.

## States & Transitions

```text
┌──────────────┐  connect_ok  ┌──────────────┐  sub_ok  ┌────────────┐     msg     ┌──────────────┐   ack_ok   ┌────────────────┐
│ Initializing │─────────────▶│  Connecting  │─────────▶│ Subscribed │────────────▶│  Executing   │───────────▶│ Acknowledging  │
└──────┬───────┘              └──────┬───────┘         └──────┬─────┘   retain     └──────┬──────┘             └──────┬─────────┘
       │ build_err                   │ conn_err               │ skip                │ exec_err                    │ ack_err
       ▼                             ▼                        ▼                     ▼                             ▼
┌──────────────┐        retry      ┌──────────────┐   re-subscribe   ┌──────────────┐   log+drop   ┌──────────────┐ log+retry ┌────────────┐
│  BuildFailed │──────────────────▶│ Reconnecting │◀─────────────────│    Backoff    │◀────────────▶│  Executing   │◀─────────▶│   Error    │
└──────────────┘   shutdown         └──────────────┘                   └──────────────┘             └──────────────┘           └────────────┘
```

### State Descriptions
- **Initializing** — Filter chronicles for MQTT triggers, construct `MqttSubscriberConfig` (topic, QoS, retain policy, TLS files, retry settings), and spawn subscribers via the injected factory.
- **Connecting** — Establish TCP/TLS clients (rumqttc by default). Failures drop into `Reconnecting` using the configured `RetryBackoff`.
- **Subscribed** — After `subscribe(topic, qos, retain_handling)` succeeds, the runtime waits for packets via `next_message()`. Idle periods incrementally sleep (50 ms) to reduce churn.
- **Executing** — Convert MQTT messages (topic, payload, QoS, retain flag, packet id) into slot `.[0]` and run the chronicle. When `retain_handling` indicates `skip`, the runtime only ACKs (for QoS>0) and bypasses the engine.
- **Acknowledging** — For QoS 1/2 packets the runtime calls `subscriber.ack(packet_id)` after successful execution. Failures log but do not retry indefinitely; the broker will redeliver if necessary.
- **Reconnecting** — Triggered by receive errors or failed ACKs. The runtime attempts `subscriber.reconnect()` followed by `subscribe()` again. Backoff windows come from merged trigger/connector `RetrySettings`.
- **Backoff** — Helper state inside `broker_loop!` that enforces delay after errors or when no packets arrive.

## Delivery Lifecycle
1. Create an MQTT client using connector credentials/TLS material; compute `client_id` and keepalive values from the connector.
2. Subscribe to the trigger topic with configured QoS and retain strategy. `retain_handling` controls whether retained packets are executed, skipped, or only executed when new.
3. For each publish packet:
   - Increment `runtime_counters().mqtt_trigger_inflight`.
   - Decode payload via `decode_payload`/`parse_binary` into base64 + text/JSON if possible.
   - Populate metadata: topic, QoS, retain flag, timestamp, connector/client identifiers.
4. Execute the chronicle via `engine.execute`. Errors do not ACK the packet, causing broker-side retries per QoS rules.
5. ACK QoS>0 packets and decrement the inflight counter.

## Error & Retry Semantics
- Subscriber construction or subscription failures raise `MqttTriggerError::*` so misconfigurations halt the runtime early.
- Runtime receive errors trigger a reconnect + re-subscribe sequence; both failures are logged separately for triage.
- Ack failures log errors but do not crash the transport; QoS semantics eventually redeliver.
- Persistent retain-skip acknowledgements ensure brokers release QoS slots even when the engine never sees the packet.
- Shutdown tokens stop the `broker_loop!`, which finishes any in-flight execution before dropping the subscriber.

## Telemetry
- `chronicle::mqtt` logs cover received/completed/failed events, including QoS, retain flags, and packet ids.
- Runtime counters expose inflight MQTT executions for observability dashboards.
- Errors differentiate between subscriber receive failures, reconnect failures, ack failures, and retain-ack failures.

## Integration Points
- Pulls connectors from `ConnectorRegistry::mqtt`, inherits TLS paths and retry defaults from connector extras.
- Shares the `ChronicleEngine` and backpressure counters with other transports; MQTT inflight stats integrate into readiness checks.
- Each subscriber runs as a task under `TransportKind::MqttIn`, allowing health queries to indicate subscriber counts.

## Open Questions
- Should we support parallel subscriptions per chronicle to increase throughput on shared topics?
- Do we need jittered backoff/jam detection when brokers repeatedly drop retained packets immediately after connect?
