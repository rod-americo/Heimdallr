# Host Stack Manifests

This directory is reserved for host-local stack manifests. The JSON manifests
are ignored by Git because they describe concrete machines, accelerator
constraints, worker limits, and local operational assumptions.

Expected local files:

- `config/host_stack/odin.json`
- `config/host_stack/thor.json`
- `config/host_stack/ms-heimdallr.json`

Validate the current host:

```bash
.venv/bin/python scripts/check_host_stack_manifest.py
```

Validate a stored manifest without comparing it to the current machine or
current host-local pipeline JSON:

```bash
.venv/bin/python scripts/check_host_stack_manifest.py \
  --manifest config/host_stack/thor.json \
  --skip-hostname-check \
  --manifest-only
```

Manifests are repo-root-relative. Keep them free of secrets, PHI, callback
tokens, local runtime paths containing case identifiers, and concrete PACS
credentials.
