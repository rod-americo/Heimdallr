# Heimdallr Integration Contracts

This directory documents the integration surface that another application can
consume without reading Heimdallr internals.

These contracts describe the current implementation under
`heimdallr.integration`. They are intentionally narrower than a general partner
API specification: Heimdallr currently provides asynchronous job submission,
final package callbacks, and configured outbound event dispatch. It does not
provide built-in authentication, signed webhooks, partner-specific adapters, or
a synchronous result API for externally submitted jobs.

## Current Surfaces

| Surface | Direction | Transport | Current status |
| --- | --- | --- | --- |
| External job submission | external app to Heimdallr | `POST /jobs` multipart upload | implemented |
| Final package delivery | Heimdallr to external app | multipart HTTP `POST` callback | implemented when configured |
| Event dispatch | Heimdallr to configured destinations | JSON HTTP `POST` | implemented when configured |

## Contract Files

- `JOB_SUBMISSION.md`: how an external application submits a study and receives
  the final package callback.
- `EVENT_DISPATCH.md`: how configured consumers receive outbound event payloads
  such as `patient_identified`.

## Compatibility and Versioning

- The current external contract line is `event_version: 1`.
- API paths are not versioned yet.
- Additive fields may appear in JSON payloads.
- Consumers must ignore unknown JSON fields.
- Breaking changes require updates to this directory, `docs/API.md`, and
  `docs/CONTRACTS.md` in the same change.

## Security Boundary

Heimdallr currently assumes network access control is enforced outside the
FastAPI app and workers. External applications must not treat Heimdallr
callbacks as signed or authenticated unless a deployment-specific proxy adds
that behavior.

Outbound event dispatch can attach static headers and headers sourced from
environment variables through `config/integration_dispatch.json`. Final package
delivery callbacks do not currently support custom per-callback headers or
payload signing.

## Data Handling

Integration payloads can include patient identifiers and clinical metadata.
Do not publish example payloads from real cases, runtime ZIPs, callback
packages, SQLite files, logs, or host-local configuration.
