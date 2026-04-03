# Heimdallr Agentic Guidelines

This repository is frequently and autonomously manipulated by AI Agents and AI-Coding tools (Gemini, Claude, Cursor, Copilot, etc.). To prevent architectural hallucinations and preserve the applied engineering, **read and strictly follow these rules before making any code modifications.**

> [!CAUTION]
> Throughout its history, this project underwent several splits and purges. Under no circumstances should you reconstruct old services that were deleted or relocated!

## 1. Domain Scope and Repository Divide

`Heimdallr` is strictly and exclusively focused on the **open-source Radiological Image MLOps infrastructure**. It is the foundation for listening (C-STORE), format processing pipelines (DICOM → NIfTI), deterministic calculations of organic volumes, TotalSegmentator processing, and metric database interfaces.

**What Does NOT Belong in Heimdallr:**
*   Proprietary clinical support services and finalized reports drafting;
*   Prompt Engineering for LLMs (OpenAI, Anthropic, MedGemma);
*   Intelligence layers or advanced assisted image conversion routines for those APIs.

**Agents:** If you are instructed to deal with LLMs, NLP, or "intelligent" routines for final reports, **STOP**. The domain of these activities belongs universally to the client repository architected as **`Asha`**.

## 2. Cloud-Native Paradigms / 12-Factor App

*   **Zero `.env` files**: Never create, expect to find, or install dependencies related to `python-dotenv`. The architecture was refactored with a cloud-native mindset. Sensitive values (like `TOTALSEGMENTATOR_LICENSE`) will be read via `os.getenv` through external injection from the Host System, Launchd/Systemd, or Docker, and **never** from an `.env` artifact.
*   **Clean Imports and JSON Settings**: Never recreate or look for the legacy `app.py`, `run.py`, or `config.py` files scattered in the root. The entire native configuration base and shared libraries live under modular components inside the `heimdallr/` package or in JSON files within `config/`.

## 3. Commit Guidelines

Always write commit messages in `EN-US` using strict semantic markings: *(use imperative mood).*
Format: `type(scope): summary`

**Allowed Types:**
*   `feat`: Strict new features
*   `fix`: Breakages or unpredictable behaviors
*   `docs`: Exclusive for root material (Markdown)
*   `refactor`: Clean internal (architectural) adjustments
*   `test`: Verifications and isolated proof scripts
*   `chore`: Tooling, versioning, or environment updates in `requirements/`
*   *(perf, ci, build, revert).*

> [!WARNING]
> Keep subject lines limited to *72 characters* and under no circumstances attach PHI/PII information into Git commits.

## 4. Directory Handling
*   **Root Cleanliness**: The ecosystem is polished with community standards residing in `.github` (CODEOWNERS, CONTRIBUTING, SECURITY).
*   Do not recreate transient traces like `.tmp`, `.pycache_local` or loose queue folders in the root (e.g., `/output/`, `/data/`). Auto-generated folders must be properly silenced in `.gitignore`.
