# Envoy → Artefact Mapping

Envoy's data-plane concepts map onto the seven processing artefacts + the
Resource model. The scope is *semantics-first* — the artefact model and
executable routing — not wire-level data-plane parity (xDS transport, SDS/SPIFFE,
HTTP/2/3 load balancing, outlier detection).

| Envoy concept | Unified artefact | Status |
|---|---|---|
| **Listener** | Resource contract (`RESOURCE_KIND_LISTENER`), bound to a Pipeline input | contract type ✓; binding model ✓ |
| **Filter chain** | **Pipeline** | executable ✓ (the pipeline module) |
| **Filter** | Pipeline **stage** (`call`) | ✓ |
| **Route configuration** | **Decision** (selects the upstream cluster) | executable ✓ |
| **Cluster** | Upstream **Resource** (`RESOURCE_KIND_UPSTREAM`), an effect target | effect ✓; binding `ENVOY_CLUSTER` ✓ |
| **Endpoint discovery (EDS)** | Resource discovery binding | contract ✓; live discovery later |
| **SDS** | Secret-provider Resource (`RESOURCE_KIND_SECRET`) | contract ✓; provider later |
| **Health checks** | Resource lifecycle policy | modeled; runtime later |
| **Retry** | Pipeline effect policy (`StagePolicy.max_retries`) | field ✓; retry belongs to the graph and the connector (see [conformance](../conformance.md#placement)) |
| **Circuit breaker** | Resource operational policy | modeled; runtime later |
| **Rate limiting** | Decision + Aggregation + Resource | Decision ✓; Aggregation ✓ |
| **Access logging** | Telemetry Resource (`RESOURCE_KIND_TELEMETRY`) | contract ✓ |
| **xDS** | Module + Resource distribution binding | OCI Module ✓; live xDS transport later |

## Executable proof

The route-decision suite
(`tests/harness/tests/decision_suites/decision.rs`) runs a request through a
route Decision that picks a cluster, on the device core that executes it:

```text
request ─▶ [route: Decision]  request.path_class == 1 ? CLUSTER_API : CLUSTER_WEB
                │
                └─▶ [response: effect @clusters.dispatch(route)] ─▶ Response{served_by: cluster}
```

- `a_route_decision_selects_the_api_cluster_for_the_api_path_class` —
  `path_class == 1` → cluster 10.
- `a_route_decision_falls_through_to_the_web_cluster` — else → cluster 20.
- `route_rules_are_first_hit_so_a_more_specific_route_must_come_first` — rule
  order is the routing precedence.

The routing decision, the filter chain, and the upstream selection are all
expressed as the same artefacts used for ordinary data processing — there is no
separate "proxy" abstraction, which is the point of the mapping.

## Related Documentation

- [../architecture/model.md](../architecture/model.md) — the artefact model
- [authoring.md](authoring.md) — authoring the mapped artefacts
