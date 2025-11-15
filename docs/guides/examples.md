# Chronicle Example Playbook

Use this guide to keep every example consistent, lean, and domain agnostic. The
canonical YAML lives under `examples/`; this document explains how to read and
extend those files without spawning duplicate markdown per connector.

## 1. Canonical Domain
All walkthroughs use the same neutral record schema:

```yaml
record:
  id: <string>                # unique identifier for the item being processed
  attributes:
    category: <string>        # optional grouping/tier information
    tier: <string>
  metrics:
    latency_ms: <number>
    retries: <number>
  observed_at: <RFC3339 timestamp>
```

Triggers emit the inbound payload as `.[0].record.*`; each phase writes its slot
and the subsequent phase references it via jaq expressions (e.g.,
`.[1].record.metrics.latency_ms`). Stick to these field names unless the spec
introduces a new canonical structure.

## 2. Template
Start new samples from `examples/spec_connectors.yaml`, which demonstrates every
connector/phase pairing. Keep this flow:

1. **Connectors** — declare shared endpoints once with descriptive names
   (`http_ingest`, `analytics_kafka`, `postgres_ledger`, `mqtt_gateway`).
2. **Trigger** — reference the inbound connector, specify options, and document
   QoS/backpressure knobs inline via YAML comments when needed.
3. **Phases** — use descriptive names (`enrich_record_http`, `publish_record_kafka`)
   and reference slots consistently. Payloads should embed part of the canonical
   `record` so downstream readers can follow the story.

## 3. Curated Example Set
Each YAML file in `examples/` focuses on a single integration pattern using the
domain model above. Keep the scenarios aligned with these “golden” examples:

| File | Scenario | Highlights |
| --- | --- | --- |
| `examples/http_to_kafka.yaml` | HTTP ingest → summary transform → Kafka publish + HTTP response | Demonstrates request handling, slot projection, producer warm-up, and response shaping. |
| `examples/http_to_grpc.yaml` | HTTP ingest → envelope shaping → gRPC ingest call | Shows how to normalize the neutral record schema and push it to a remote gRPC service with trace propagation. |
| `examples/kafka_to_email.yaml` | Kafka summaries → alert email fan-out | Shows Kafka consumer triggers, text+HTML email payload construction, and SMTP delivery. |
| `examples/grpc_enrichment.yaml` | HTTP ingest → gRPC enrichment → ACK | Covers calling external RPC services with metadata propagation before acknowledging the request. |
| `examples/spec_connectors.yaml` | Connector/phase reference matrix | Catalog of every supported connector and phase; use it as the starting template for new samples. |

Schemas referenced by the examples live under `examples/schemas/`
(`alert.proto`, `sensor_reading.avsc`, etc.). When changing a schema update the
source definition and any generated descriptors in the same commit so the YAML
remains reproducible.

## 4. Style Rules
- Reuse the canonical connector names across files so logs/tests stay familiar.
- Keep all explanatory prose inside this document; add only terse YAML comments
  when absolutely necessary.
- When adding a new example, reference it here with a one-line description and
  update the relevant spec section if new behavior is showcased.

## 5. Contribution Checklist
1. Copy `examples/spec_connectors.yaml` or another existing file as a starting
   point.
2. Update connector/phase names but keep the `record.*` object structure.
3. Run `cargo fmt` + `cargo test --all-features` to ensure fixtures compile.
4. Mention the new example in this document so readers can find it.

Keeping everything in one lean file avoids documentation creep while preserving
technical accuracy. For detailed semantics, always defer to
`docs/specification.md`.***
