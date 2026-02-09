# Validation Stage Manual

## Scope

This manual defines the operational meaning of two Heimdallr stages:

- `validation-ready`
- `production-candidate`

It is designed for the current project reality: single maintainer, MVP transition, and semi-production validation in controlled clinical workflows.

## Stage Definitions

### `validation-ready`

A module is **ready to run in semi-production** under a clear protocol.

It is not yet considered stable enough for institutional rollout discussions.

### `production-candidate`

A module has **passed repeated practical validation**, with traceability and tested rollback.

It is ready for institutional/equipment/team discussion as a deployment candidate.

## Gate Checklist: `validation-ready`

All items must be true.

1. Clinical scope and boundaries are explicit.
2. Input/output contract is documented.
3. Deterministic behavior is verified for same input/version.
4. Minimum quality metrics are defined and measured.
5. Data protection checks are documented (no unintended PHI leakage).
6. Failure behavior and fallback path are documented.
7. Basic observability exists (logs + run notes).
8. A validation protocol is written and executable.

## Gate Checklist: `production-candidate`

All items must be true.

1. Module is already `validation-ready`.
2. Validation was repeated across multiple real-use sessions.
3. Results are traceable (date, context, module version, operator decision).
4. Quality metrics are stable across repeated runs.
5. Rollback was tested and documented.
6. Known limitations and contraindications are documented.
7. Go/No-Go summary is prepared for institutional review.

## Required Evidence Package

Store one package per module per validation cycle.

Suggested location:

`docs/validation-evidence/<module-id-or-name>/<yyyy-mm-dd>/`

Minimum files:

1. `protocol.md`
2. `run-log.md`
3. `metrics.csv` or `metrics.md`
4. `failure-cases.md`
5. `rollback-test.md`
6. `decision.md`

## Semi-Production Validation Protocol (Solo Operator)

1. Select target module and fixed version/hash.
2. Define cohort/sample and inclusion criteria.
3. Execute run in controlled VPN workflow.
4. Record outputs and timing.
5. Review edge cases and failures.
6. Compare against expected behavior/clinical intent.
7. Fill decision block (`stay`, `promote`, or `rollback`).

## Decision Rules

### Promote to `validation-ready`

Promote only if the stage checklist is fully satisfied and no blocking safety issue remains open.

### Promote to `production-candidate`

Promote only if repeated practical validation confirms stable behavior, traceability is complete, and rollback test passed.

## Decision Template

Use this block in `decision.md`:

```md
## Validation Decision

- Module:
- Version:
- Stage requested: validation-ready | production-candidate
- Date:
- Context/service:
- Reviewer:

### Summary
- Expected behavior achieved:
- Critical issues:
- Non-critical issues:

### Metrics Snapshot
- Metric 1:
- Metric 2:
- Metric 3:

### Rollback
- Tested: yes/no
- Result:

### Decision
- Outcome: promote | hold | rollback
- Next actions:
```

## Notes

- `implemented` in code does not automatically mean `validation-ready`.
- This process is intentionally lightweight but auditable, suitable for a one-person team in MVP progression.
