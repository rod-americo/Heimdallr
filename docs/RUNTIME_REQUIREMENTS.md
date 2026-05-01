# Runtime Requirements

This document explains how `requirements.txt` should be treated when rebuilding
or auditing a Heimdallr runtime. It also records the current POC host baseline
observed on `thor`.

## 1. Canonical Runtime Contract

The canonical dependency file is:

```text
requirements.txt
```

The Python version contract is declared in `pyproject.toml`:

```text
>=3.12,<3.13
```

Use Python 3.12 for runtime rebuilds. Do not treat a locally working Python
3.13 or 3.14 environment as the supported target unless `pyproject.toml`,
tests, and host operations are deliberately updated.

## 2. POC Host Baseline

The current POC test host is:

```text
thor
```

The current in-repository POC venv on `thor` is:

```text
/home/rodrigo/Heimdallr/.venv
```

Observed baseline:

```text
python: 3.12.3
pip: 24.0
pip check: No broken requirements found.
requirements audit: matches requirements.txt
```

Historical drift observed in the older external POC venv
`/home/rodrigo/venvs/totalsegmentator`:

| Item | Observed state | Operational interpretation |
| --- | --- | --- |
| `textual==8.1.1` | missing from POC venv | Affects `heimdallr.tui`; core pipeline tests may still pass without exercising TUI runtime. |
| `xgboost==2.1.4` | `3.1.3` installed | The POC venv is newer than the pinned requirement; do not copy this drift into `requirements.txt` without testing affected metrics. |
| `python-dotenv==1.0.1` | installed as extra | Do not add this to `requirements.txt`; `.env` loading remains outside Heimdallr architecture. |

Use `~/Heimdallr/.venv` for new code tests unless the user explicitly asks to
compare against the older external venv.

## 3. Git Parity Rule: Local and Thor

Before using `thor` for code tests, local and `thor` must point at the same
branch and commit, with no unexpected worktree changes.

Local check:

```bash
git status --short --branch
git rev-parse --short HEAD
```

Thor check:

```bash
ssh thor 'cd ~/Heimdallr && git status --short --branch && git rev-parse --short HEAD'
```

Expected rule:

- branch names must match
- upstream tracking must match the intended remote branch
- commit hashes must match before comparing test results
- worktrees must be clean unless the active task explicitly requires a known
  local-only change

When local code changes should be tested on `thor`, push the branch first and
then update `thor` with a fast-forward pull:

```bash
git push
ssh thor 'cd ~/Heimdallr && git pull --ff-only'
```

Do not edit code directly on `thor` unless the user explicitly requests host-side
debugging.

## 4. Requirements Audit Command

Use the active environment's Python to compare installed packages against
`requirements.txt`.

Local:

```bash
.venv/bin/python scripts/check_runtime_requirements.py
```

Thor POC venv:

```bash
ssh thor 'cd ~/Heimdallr && .venv/bin/python scripts/check_runtime_requirements.py'
```

The command exits non-zero when required packages are missing or pinned versions
do not match. Extra packages are reported only when they are operationally
important or when `--show-extras` is passed.

## 5. Rebuild Procedure

Do not alter the POC host venv unless the user explicitly asks for a rebuild.
The current project venv path on `thor` is `~/Heimdallr/.venv`.

When a separate experimental environment is needed, prefer creating a new venv
path rather than overwriting the known working one.

Example new environment:

```bash
python3.12 -m venv /home/rodrigo/venvs/heimdallr-YYYYMMDD
source /home/rodrigo/venvs/heimdallr-YYYYMMDD/bin/activate
python -m pip install --upgrade pip
python -m pip install -r ~/Heimdallr/requirements.txt
python -m pip check
python ~/Heimdallr/scripts/check_runtime_requirements.py
```

Only promote the new venv after:

- `pip check` passes
- `scripts/check_runtime_requirements.py` passes or all drift is documented
- relevant Heimdallr tests pass on `thor`
- TotalSegmentator task smoke is verified when segmentation behavior is in
  scope

## 6. Requirements Review Notes

- Keep `requirements.txt` as the rebuild source of truth.
- Do not add `python-dotenv`.
- Do not update pins from a working host venv without confirming code impact.
- CUDA-related packages are Linux x86_64 specific and are guarded by environment
  markers in `requirements.txt`.
- TUI support requires `textual==8.1.1` in the selected venv.
- Default presentation locale is `en_US`; use `pt_BR` only through an explicit
  host-local override or targeted i18n tests.
