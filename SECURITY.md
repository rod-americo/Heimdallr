# Security Policy

## Scope

This project handles medical imaging workflows and may process sensitive healthcare data in deployment environments.
Security issues are treated as high priority.

## Reporting a Vulnerability

Please do not open public issues for suspected vulnerabilities.

Report privately to:
- `security@heimdallr-project.org` (preferred)
- or repository maintainers through a private channel

Include, when possible:
- Affected component and version/commit
- Reproduction steps
- Impact assessment (confidentiality/integrity/availability)
- Suggested mitigation

## Initial Response Targets

- Acknowledgement: within 3 business days
- Triage status update: within 7 business days
- Mitigation plan or workaround: as soon as validated

## Coordinated Disclosure

We follow coordinated disclosure:
- Reporter and maintainers agree on a fix timeline
- Public disclosure happens after mitigation is available, or after an agreed deadline

## Supported Security Baseline

Security support is best-effort for the current `main` branch.
Older commits and forks may not receive backported fixes.

## Deployment Responsibility

Operators are responsible for:
- Network segmentation and firewall rules
- Access control and secrets management
- Audit logging retention and monitoring
- Compliance validation (LGPD/GDPR/HIPAA-like contexts, as applicable)
