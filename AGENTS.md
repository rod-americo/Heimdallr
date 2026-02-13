# Agent Instructions

## Language Policy
- For repository artifacts, use `EN-US`:
  - Documentation files under `docs/`
  - Source code text (identifiers, string literals when feasible)
  - Code comments
  - Commit messages and pull request text
- For chat interaction with the user, use `PT-BR`:
  - User prompts can be in `PT-BR`
  - Assistant responses in chat must be in `PT-BR`

## Precedence
- If a specific task explicitly requires another language, follow the task requirement for that scope only.
- Otherwise, apply the default policy above.

## Scope and Instruction Order
- Apply this file to all work inside this repository.
- Instruction priority order:
  - Direct user instruction for the current task
  - Repository `AGENTS.md`
  - Default agent behavior

## Change Management
- Keep changes minimal, targeted, and within requested scope.
- Do not perform unrelated refactors without explicit request.
- Do not modify unrelated files.
- Preserve existing behavior unless the task explicitly requires behavior change.

## Response and Reporting
- For implementation tasks, report:
  - Files changed
  - What was changed
  - Validation performed
- If tests/build/lint were not run, state that explicitly.

## Testing and Validation
- Run the smallest relevant test/lint set for impacted areas first.
- Do not ignore failing checks; report failure cause and impact.
- Prefer reproducible commands for validation.

## Python Environment
- For Python tasks in this repository, use the virtual environment at `venv/`.
- Prefer invoking binaries directly from the virtual environment:
  - `venv/bin/python`
  - `venv/bin/pip`
- If activation is required, use `source venv/bin/activate` before running Python tooling.
- If `venv/` is missing, create it with `python3 -m venv venv` before installing or running Python dependencies.

## Git Policy
- Use `EN-US` commit messages in imperative mood.
- Keep commit subjects concise and descriptive.
- Keep one logical change per commit.
- Do not use destructive git commands unless explicitly requested.
- Use semantic commits in the `type(scope): summary` format.
- Allowed commit types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `perf`, `ci`, `build`, `revert`.
- Keep the commit subject at 72 characters or less.
- Use a short, stable scope when applicable (for example: `pipeline`, `docs`, `api`, `deid`, `dicom`).

## Security and Privacy
- Never include PHI/PII in code, docs, examples, logs, commits, or PR text.
- Prefer anonymized/synthetic examples in documentation.
- Avoid exposing secrets, credentials, local tokens, or internal endpoints.

## Documentation Conventions
- For files under `docs/`, prefer this section order when applicable:
  - `Context`
  - `Decision`
  - `Implementation`
  - `Risks`
  - `Validation`
  - `Rollback`

## Definition of Done
- A task is complete only when all applicable items are satisfied:
  - Requested scope implemented
  - Relevant validation executed (or limitation declared)
  - Documentation updated when behavior/process changes
  - Clear rollback path exists for operationally relevant changes
