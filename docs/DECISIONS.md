# Decisions

This file records lightweight architectural decisions that affect how Heimdallr
should evolve.

## How to Add a Decision

Each decision should include:

- date
- context
- decision
- impact
- tradeoff
- alternatives rejected

## Decisions

### 2026-05-01 - Preserve `heimdallr/` as the main package

**Context**

The starter kit recommends a clean package root and allows `src/` only when it
is a conscious packaging requirement. Heimdallr already has a maintained
`heimdallr/` package with many module entrypoints, tests, imports, docs, and
runtime assumptions.

**Decision**

Keep production code under `heimdallr/` and do not migrate to `src/heimdallr/`
during structural recovery.

**Impact**

- Avoids a broad import and packaging migration unrelated to current risk.
- Keeps existing tests, module entrypoints, and operational commands stable.

**Tradeoff**

- The package does not mirror every starter-kit layer name.
- Structural clarity must be documented through real module boundaries instead
  of a cosmetic directory move.

**Alternatives rejected**

- Move all code to `src/heimdallr/` during recovery.
- Split workers into new repositories without a runtime migration plan.

---

### 2026-05-01 - Use `config/doctor.json` for structural doctor policy

**Context**

Heimdallr already uses `config/` for versioned examples and operational
configuration. The starter-kit doctor policy is small, versioned, and not
host-local.

**Decision**

Store doctor policy in `config/doctor.json`.

**Impact**

- Keeps governance validation discoverable beside other config contracts.
- Avoids adding a new top-level `governance/` directory for one file.

**Tradeoff**

- `config/` now contains both runtime examples and repository governance
  policy.
- Docs must make clear that `config/doctor.json` is tracked and not a secret or
  host-local override.

**Alternatives rejected**

- Add `governance/doctor.json`.
- Rely on hardcoded script behavior with no policy file.

---

### 2026-05-01 - Prepare local hook support without installing it

**Context**

The repository is existing, may have local workflows, and the user requested a
one-off branch for this task. Forcing hook installation could disrupt the
maintainer's environment.

**Decision**

Add `.githooks/pre-commit` and `scripts/install_git_hooks.sh`, but do not run
the installer automatically.

**Impact**

- The gate can be enabled intentionally.
- This recovery round does not mutate local Git hook configuration.

**Tradeoff**

- Enforcement is available but not guaranteed on every local commit.
- CI or maintainer workflow must opt in if gate enforcement becomes mandatory.

**Alternatives rejected**

- Auto-install hooks during this task.
- Omit hook support entirely.

---

### 2026-05-01 - Document LLM-adjacent residue as out-of-scope debt

**Context**

The repository contains existing dependency and settings residue related to
external model services, while the repository boundary states that LLM,
prompting, NLP, and report intelligence belong to Asha.

**Decision**

Do not remove compatibility residue in this structural recovery round, but
document it as a boundary hotspot that must not be expanded inside Heimdallr.

**Impact**

- Avoids breaking unknown local workflows during documentation recovery.
- Makes future additions easier to reject or redirect to Asha.

**Tradeoff**

- Some dependency/config names still appear broader than the desired domain.
- A later cleanup may be needed to remove unused LLM-adjacent dependencies after
  runtime impact is confirmed.

**Alternatives rejected**

- Remove dependencies and settings immediately without impact analysis.
- Treat the residue as permission to add intelligence workflows here.
