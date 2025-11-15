# Chronicle Security Guide

This guide consolidates Chronicle’s current security posture and operational
expectations. It replaces scattered TLS/auth notes and upgrade caveats so teams
can harden deployments without trawling historical docs. Pair it with
`docs/specification.md` for configuration schemas and `docs/guides/operations.md` for
runtime procedures.

## 1. Principles
- **Single source of truth**: All security-relevant settings (cert paths, auth
  credentials, feature flags) live in the integration YAML or environment, not
  sprinkled through code.
- **Least privilege**: Chronicle connectors should authenticate with scoped
  credentials (e.g., per-topic Kafka users, DB roles limited to needed schemas).
- **Immutable builds**: Ship signed release artifacts and avoid mutating the
  runtime container/image after deployment.

## 2. Transport Security
Chronicle currently parses TLS/auth blocks for every connector type. Ensure the
following combinations are respected in configs and infrastructure:

| Connector | Plaintext | TLS/mTLS | Auth Notes |
| --- | --- | --- | --- |
| HTTP server/client | Default is plaintext. Terminate TLS at ingress or set `tls.{cert,key,ca}` once native TLS lands. | Client certs require TLS; supply CA + cert/key. | Basic auth via `auth.username/password`; header-based tokens supported via jaq expressions. |
| Kafka | `security.protocol=PLAINTEXT` when no TLS/SASL supplied. | Provide `security.tls.ca` plus optional client cert/key to enable SSL/SASL_SSL. | Username/password + mechanism under `security.sasl.*`. |
| RabbitMQ | `amqp://` for plaintext. | Use `amqps://` or `tls.*` block; client cert/key imply TLS. | Credentials may live in URI; rotate secrets via env vars referenced in YAML. |
| MQTT | `mqtt://` for plaintext. | `mqtts://` or `tls.*` for TLS/mTLS. | Username/password optional when mTLS enabled. |
| Redis | `redis://` vs `rediss://`. | TLS block accepts CA/cert/key. | Use ACL users with scoped permissions. |
| MongoDB | `mongodb://` vs `mongodb+srv://` or `options.tls.*`. | Provide CA/cert/key; Chronicle preserves paths for the driver. | Authentication via URI or `options.auth`. |
| SQL (MariaDB/Postgres) | DSN without TLS extras. | Supply `tls` object (CA/cert/key) or TLS-specific DSN schemes. | Prefer dedicated DB roles with least privilege. |

General recommendations:
- Store cert/key material alongside the integration file with restrictive file
  permissions; Chronicle only reads them at startup.
- Use environment-variable interpolation for secrets (`${VAR}`) so repos never
  carry raw credentials.
- Rotate broker/database credentials regularly and restart Chronicle to pick up
  changes (hot reload is not yet implemented).

## 3. Management Surface
- Expose the management server on a private network segment. If unavoidable,
  enforce TLS/mTLS via your ingress and require auth (basic/OIDC) before proxying
  to Chronicle.
- `/status` leaks route/endpoint inventory; treat it as sensitive operational
  data.
- `/metrics` can reveal connector counts and topic names; avoid making it
  world-readable.

## 4. Supply Chain & Dependencies
- Track dependency drift with `tools/audit-deps.sh`; pay attention to security
  advisories flagged by `cargo audit` (add it to the script if not already).
- Keep feature-gated crates (`rdkafka`, `lapin`, etc.) disabled unless the
  environment needs them to shrink attack surface and compile times.
- Record release hashes and provenance; if you distribute binaries, sign them
  before uploading to artifact storage.

## 5. Incident Response Checkpoints
- Maintain an inventory of connectors with associated credential owners and
  rotation cadences.
- Know how to revoke access quickly: disable connector flags, remove YAML
  definitions, or rotate secrets and redeploy.
- For suspected config compromise, rehydrate chronicles from clean YAML and
  compare against git history; avoid editing configs in-place on servers.

Future enhancements (tracked in `planning/backlog.md`):
- Enforce TLS/auth validation at load time so plaintext-only deployments require
  explicit opt-in.
- Add authentication/authorization to the management server.
- Support secret stores (AWS/GCP vaults) instead of relying solely on env vars.

Until those land, follow this guide and your organization’s standards. Do not
reintroduce version-history sections here—keep it current and concise.***
