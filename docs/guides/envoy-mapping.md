# Envoy → Artefact Mapping

Phase 3 implements the spec's **SERVICE-MESH MAPPING** at the semantic level:
Envoy's data-plane concepts map onto the seven processing artefacts + the
Resource model. This is the *semantics-first* scope — the artefact model and
executable routing — not wire-level data-plane parity (xDS, SDS/SPIFFE,
HTTP/2/3 load balancing, outlier detection), which is a later workstream.

| Envoy concept | Unified artefact | Status |
|---|---|---|
| **Listener** | Resource contract (`ResourceKind::LISTENER`), bound to a Pipeline input | contract type ✓; binding model ✓ |
| **Filter chain** | **Pipeline** | executable ✓ (`chronicle-pipeline`) |
| **Filter** | Pipeline **stage** (`call`) or runtime extension | ✓ |
| **Route configuration** | **Decision** (selects the upstream cluster) | executable ✓ |
| **Cluster** | Upstream **Resource** (`ResourceKind::UPSTREAM`), an effect target | effect ✓; binding `ENVOY_CLUSTER` ✓ |
| **Endpoint discovery (EDS)** | Resource discovery binding | contract ✓; live discovery later |
| **SDS** | Secret-provider Resource (`ResourceKind::SECRET`) | contract ✓; provider later |
| **Health checks** | Resource lifecycle policy | modeled; runtime later |
| **Retry** | Pipeline effect policy (`StagePolicy.max_retries`) | field ✓; executor honoring later |
| **Circuit breaker** | Resource operational policy | modeled; runtime later |
| **Rate limiting** | Decision + Aggregation + Resource | Decision ✓; Aggregation is Phase 4 |
| **Access logging** | Telemetry Resource (`ResourceKind::TELEMETRY`) | contract ✓ |
| **xDS** | Module + Resource distribution binding | OCI Module is Phase 5 |

## Executable proof

`modules/app/decision/tests/decision.rs` runs a request through a route
Decision that picks a cluster — the routing half, now on the device core that
actually executes it:

```text
request ─▶ [route: Decision]  request.path_class == 1 ? CLUSTER_API : CLUSTER_WEB
                │
                └─▶ [response: effect @clusters.dispatch(route)] ─▶ Response{served_by: cluster}
```

- `api_path_routes_to_api_cluster` — `path_class == 1` → cluster 10.
- `other_paths_route_to_web_cluster` — else → cluster 20.
- `filter_chain_builds_a_sealed_pipeline_artefact` — the chain is a sealed,
  content-addressed `Pipeline` artefact whose stages reference the Decision
  (call) and the cluster Resource (effect).

The routing decision, the filter chain, and the upstream selection are all
expressed as the same artefacts used for ordinary data processing — there is no
separate "proxy" abstraction, which is the point of the mapping.

## Related Documentation

- [../architecture/model.md](../architecture/model.md) — the artefact model
- [authoring.md](authoring.md) — authoring the mapped artefacts
