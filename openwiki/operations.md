---
type: Runbook
title: Operations
description: Dependency management with uv, pre-commit, CI checks, the documentation rule and releases.
tags: [ci, release, uv, pre-commit, documentation]
---

# Operations

## Dependencies: uv

Dependencies are declared in `pyproject.toml` and pinned in `uv.lock`.

```bash
uv sync                    # core + dev dependencies, in .venv
uv sync --extra flink      # also install PyFlink
uv add <package>           # add a dependency; updates pyproject.toml and uv.lock
uv run pytest
```

Python 3.10 to 3.12 is supported; PyFlink does not support 3.13.

## Pre-commit

```bash
uv run pre-commit install          # run the checks on every commit
uv run pre-commit run --all-files
```

The hooks lint and format with ruff, run basic file checks (YAML and TOML
syntax, trailing whitespace, merge-conflict markers, large files) and check that
`uv.lock` matches `pyproject.toml`. The training notebooks, the sample data and
the slide deck are skipped.

## CI on pull requests

`.github/workflows/ci.yml` runs four checks:

| Check | What it verifies |
|---|---|
| Pre-commit | the hooks above |
| Unit tests | the pytest suite with Spark and Flink; starts once pre-commit passes |
| Version | the `version` in `pyproject.toml` has not been released yet |
| Documentation | code changes come with a documentation change |

## Documentation rule

A pull request that changes the framework or its configuration (`jobs/`,
`utils/`, `etl/`, `config/`, `main.py`) must also change `README.md` or a page
under `openwiki/`. `scripts/check_docs_updated.py` implements the check and
lists the files that triggered it.

The pages can be edited by hand. They follow the layout of
[OpenWiki](https://docs.langchain.com/oss/openwiki/overview), so they can also
be refreshed with it before opening a pull request:

```bash
npm install -g openwiki
openwiki --update          # needs a model provider API key
```

`openwiki/INSTRUCTIONS.md` is the brief OpenWiki follows, and `.openwikiignore`
lists the paths it must not read.

## Releases

Every merge to `main` is a release. `.github/workflows/release.yml` tags the
merged commit `v<version>` and publishes a GitHub release with generated notes.
The version is read from `pyproject.toml`, so every pull request bumps it:

```bash
uv version --bump patch    # or minor / major; commit pyproject.toml and uv.lock
```

The Version check fails a pull request whose version already has a tag.
