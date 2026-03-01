---
name: git_commit
description: Instructions for creating git commits following repository policy.
---

# Git Commit Skill

Use this skill whenever you need to commit changes to this repository.

## Language Policy
- All commit messages must be in `EN-US`.
- Use the imperative mood (e.g., "Add feature" instead of "Added feature").

## Format: Semantic Commits
Use the format: `type(scope): summary`

### Allowed Types
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `test`: Adding missing tests or correcting existing tests
- `chore`: Changes to the build process or auxiliary tools and libraries
- `perf`: Code change that improves performance
- `ci`: Changes to CI configuration files and scripts
- `build`: Changes that affect the build system or external dependencies
- `revert`: Reverts a previous commit

### Constraints
- **Scope**: Use a short, stable scope (e.g., `api`, `docs`, `prompts`, `ui`).
- **Length**: Keep the subject line at 72 characters or less.
- **Content**: One logical change per commit.
- **Privacy**: Never include PHI/PII or secrets in commit messages.

## Examples
- `feat(api): add endpoint for patient validation`
- `fix(prompts): correct bilateral effusion logic`
- `docs(readme): update installation instructions`
- `refactor(core): simplify metric calculation`
