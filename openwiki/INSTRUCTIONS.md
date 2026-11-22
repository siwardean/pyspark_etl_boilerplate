# Instructions for the wiki

This file is the brief for the `openwiki/` documentation. It is written by the
maintainers; OpenWiki reads it on every run and never rewrites it.

## Audience

Data engineers who write jobs with this framework, and trainers who run the
DreamAirlines training. Assume Python and Spark basics, not knowledge of the
codebase.

## Scope and priorities

1. How to write and run a job (`JobBatch`, `JobStream`, the session mixins).
2. Configuration: the YAML sections and the typed `Settings` / `Params` models.
3. What is tracked in MLflow and what is logged.
4. How the repository is operated: uv, pre-commit, CI, releases.
5. The training material, at overview level only. The notebooks document themselves.

## Conventions

- One topic per page; link between pages instead of repeating content.
- Show short, runnable examples taken from `jobs/examples/` or the tests.
- State limits plainly (for example what is not tracked on Flink).
- Do not document the sample data files or the slide deck in detail.
