#!/usr/bin/env bash
set -euo pipefail

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "This directory is not a Git repository." >&2
  echo "Run 'git init' before installing hooks." >&2
  exit 1
fi

git config core.hooksPath .githooks
echo "Git hooks installed in .githooks"
