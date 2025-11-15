# HTTP Trigger Runtime State Machine

## Goals
- Fulfil `docs/specification.md` §3.2 HTTP Trigger guarantees: route-method matching, structured payload shape, and adherence to connector `content_type`/`path`.
- Provide predictable lifecycle hooks for readiness and graceful shutdown of each Axum server spawned per connector.
- Surface failures (binding errors, dispatcher issues) with actionable telemetry so operators can distinguish between routing mistakes and downstream execution faults.

## States & Transitions

```text
┌──────────────┐   bind_ok    ┌────────────┐   request   ┌───────────────┐  dispatch_ok  ┌──────────────┐
│ Initializing │─────────────▶│ Listening  │────────────▶│   Executing    │─────────────▶│  Responding   │
└──────┬───────┘              └──────┬─────┘             └───────┬────────┘              └──────┬───────┘
       │ bind_err                    │ shutdown                   │ exec_err                     │ send_err
       ▼                             ▼                            ▼                              ▼
┌──────────────┐          cancel     ┌────────────┐     exec_fail ┌──────────────┐      retry?   ┌────────────┐
│  BindFailed  │────────────────────▶│  Draining  │──────────────▶│  Dispatching │──────────────▶│ ErrorReply │
└──────────────┘   all_tasks_done    └────────────┘  dispatch_err └──────────────┘                └────────────┘
```

### State Descriptions
- **Initializing** — Iterate configured chronicles, group routes per `SocketAddr`, and spawn one Axum server per connector using `TaskTransportRuntime`. Failures to parse method/path or bind the socket push the instance into `BindFailed`.
- **Listening** — Bound TCP listener accepts any HTTP method via `Router::route(..., any(handler))`. Incoming requests move the instance to `Executing`. Shutdown tokens transition to `Draining`.
- **Executing** — Build the trigger payload (headers, path/query maps, JSON-or-binary body, metadata timestamps) and call `ChronicleEngine::execute`. Success proceeds to `Dispatching`; execution errors jump straight to `Responding` with `500`.
- **Dispatching** — Convert the execution plan into concrete actions via `ActionDispatcher`. Failures surface as `502 Bad Gateway`. Successful dispatches transition to `Responding`.
- **Responding** — Encode the `ChronicleExecution.response` (if present) or return `202 Accepted`. Errors while writing back (rare) produce generic `500` responses but do not crash the server.
- **Draining** — Triggered by shutdown tokens. Stops accepting new requests but allows in-flight Axum tasks to complete before closing the listener.

## Delivery Lifecycle
1. **Route match** — Look up path+method in the per-listener map built from trigger `options`.
2. **Body ingestion** — Read the request body up to 1 MiB (`MAX_BODY_BYTES`). Bodies marked as JSON are deserialized; everything else becomes base64 in the `body` field.
3. **Payload assembly** — Populate slot `.[0]` with `headers`, `query`, `path`, `metadata.received_at`, and `request.*` legacy mirrors.
4. **Engine execution** — Invoke `engine.execute(chronicle, payload)`; the result carries actions, delivery policy, and optional HTTP response metadata.
5. **Action dispatch** — `ActionDispatcher` streams the actions respecting delivery policy backpressure.
6. **Response** — Encode the execution response (status/body/content-type) or emit `202` if the chronicle has no response phase.

## Error & Retry Semantics
- Bind failures are logged once per connector; there is no auto-retry because misconfiguration is assumed.
- Route misses return `404`/`405` immediately to conserve resources.
- Payload parsing errors (e.g., invalid JSON) short-circuit with `400` while the transport stays healthy.
- Dispatcher failures keep the server alive but increment error telemetry and respond with `502`.
- Shutdown is graceful: the `CancellationToken` waits for Axum to finish pending requests before closing sockets.

## Telemetry
- `http trigger runtime listening` and `failed to bind` log lines show connector-host readiness.
- Per-request logs differentiate execution vs. dispatch failures, including chronicle names for rapid triage.
- `MAX_BODY_BYTES` breaches and payload parsing errors emit structured errors so operators can tune connectors.

## Integration Points
- Requires `ConnectorFactoryRegistry::http_server` for binding info and TLS (future) settings.
- Shares the global `ActionDispatcher` and `ChronicleEngine` used by other transports, so delivery semantics stay uniform.
- Runs under `TransportKind::HttpIn`, exposing health/readiness through the standard transport API.

## Open Questions
- Should we add per-route rate-limits or integrate with the engine’s backpressure semaphore to keep HTTP triggers from overwhelming the runtime?
- Do we need configurable body-size limits per connector instead of the hard-coded 1 MiB cap?
