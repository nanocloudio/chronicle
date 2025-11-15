# MongoDB Trigger Runtime State Machine

## Goals
- Meet `docs/specification.md` §3.2 MongoDB Trigger promises: change-stream coverage, optional collection/pipeline scoping, and resume token durability.
- Offer deterministic handling of change-stream disconnects so resumable errors do not lose events.
- Keep resume-token persistence side effects isolated from the core runtime to avoid blocking event processing.

## States & Transitions

```text
┌──────────────┐  load_ok  ┌────────────────┐  client_ok  ┌──────────────┐    event    ┌──────────────┐  persist_ok  ┌────────────┐
│ Initializing │──────────▶│ LoadingResume │────────────▶│   Streaming   │────────────▶│   Executing   │────────────▶│ Persisting │
└──────┬───────┘           └──────┬────────┘            └──────┬────────┘             └──────┬───────┘              └──────┬────┘
       │ build_err                │ resume_err                  │ stream_err               │ exec_err                    │ persist_err
       ▼                          ▼                             ▼                          ▼                            ▼
┌──────────────┐       abort     ┌──────────────┐    reconnect    ┌──────────────┐  log+drop  ┌──────────────┐   retry?   ┌────────────┐
│  BuildFailed │────────────────▶│ ResumeFailed │────────────────▶│ Reconnecting │◀──────────▶│   Executing   │◀──────────▶│   Error    │
└──────────────┘   shutdown      └──────────────┘     backoff     └──────────────┘            └──────────────┘            └────────────┘
```

### State Descriptions
- **Initializing** — Filter chronicles for MongoDB triggers, load connector metadata (URI, TLS), parse options (`collection`, `pipeline`), and construct a `ResumeStore` rooted under `.chronicle/state/mongodb/`.
- **LoadingResume** — Read the last stored resume token (if any). Invalid or unreadable tokens surface as `MongodbTriggerError::InvalidStoredResume`, aborting startup so operators can fix the file.
- **Streaming** — `MongodbChangeStreamDriver` opens a database/collection stream with optional resume token. The `broker_loop` equivalent is implemented manually with `tokio::select!` to honour shutdown tokens.
- **Executing** — Convert each `ChangeStreamEvent` into slot `.[0]` (`operationType`, `ns`, `documentKey`, `fullDocument`, update descriptors, metadata). Execute the chronicle; failures are logged but do not stop the stream.
- **Persisting** — Successful executions persist the event’s resume token to disk (base64). Errors only log; the runtime keeps running because the next event provides a new token.
- **Reconnecting** — Triggered when `next_event()` yields `Err`. The runtime sleeps for the configured `RetryBackoff`, calls `reconnect()`, and re-enters `Streaming`.

## Delivery Lifecycle
1. Build `MongodbChangeStreamConfig` per chronicle (connector clone, optional collection, aggregation pipeline, resume token).
2. On startup, load/persist resume tokens via `ResumeStore`; invalid tokens stop the chronicle to avoid silent data loss.
3. `stream.next()` drives change events. `None` results cause a 200 ms idle sleep to keep CPU low while still honouring watch timeouts.
4. `build_change_payload` translates MongoDB BSON into JSON, including:
   - `operationType`
   - `ns.db` + `ns.coll`
   - `documentKey`, `fullDocument`, `updateDescription`
   - `metadata.clusterTime`, `metadata.wallTime`, `metadata.resumeToken`
5. Execute the chronicle; on success, persist the resume token and continue.

## Error & Retry Semantics
- Connector lookups and option parsing throw `MongodbTriggerError::*` variants that abort startup early.
- Change-stream driver errors trigger `backoff.on_failure()` and a reconnect cycle; backoff parameters inherit from trigger/connector extras.
- Resume persistence errors log with the file path but do not halt ingestion. Operators can monitor the log key `resume_token_persist_failed`.
- Shutdown tokens break the loop promptly, ensuring watchers exit cleanly without waiting for more events.

## Telemetry
- `chronicle::mongodb` logs capture stream errors, reconnect attempts, trigger completion/failure per event, and resume persistence failures.
- Listener counts are exposed via `MongodbTriggerRuntime::listener_count` for readiness dashboards.
- Metadata, especially `operation` and namespace, is included in log events to speed root cause analysis.

## Integration Points
- Uses `ConnectorRegistry::mongodb` for credentials/TLS; TLS options map into `mongodb::options::ClientOptions`.
- `RetrySettings` come from merged trigger `options.extra` and connector extras, aligning behaviour with other backoff-aware transports.
- The stored state lives under `.chronicle/state/mongodb`, so operators must ensure the process has write access there.

## Open Questions
- Should we surface resume lag (clusterTime vs. wall clock) as a metric to detect stalled streams?
- Do we need to add a circuit breaker when repeated resume persistence errors occur (e.g., disk full) to avoid flooding logs?
