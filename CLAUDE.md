# python-esios

## Releasing

This project uses [release-please](https://github.com/googleapis/release-please) for automated versioning and publishing.

**How it works:**

1. Write conventional commits (`feat:`, `fix:`, `chore:`, etc.)
2. On push to `main`, release-please auto-opens/updates a **Release PR** with version bump + CHANGELOG
3. Merge the Release PR → GitHub Release + tag created → PyPI publish via trusted publishing

**Key files:**

- `release-please-config.json` — release-please configuration (package type, changelog sections)
- `.release-please-manifest.json` — tracks current version
- `.github/workflows/release-please.yml` — workflow (release PR management + PyPI publish)

**Version is tracked in** `pyproject.toml` (`project.version`). Release-please bumps it automatically based on commit types (`feat:` → minor, `fix:` → patch).

**Never bump the version manually** — let release-please handle it via the Release PR.
