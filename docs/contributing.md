# Contributing

Contributions are welcome. This page covers everything you need to get started.

## Before you open a PR

- **Open an issue first** for any non-trivial change. This lets us align on
  approach before you invest significant time.
- Bug fixes and documentation improvements can go straight to a PR.

## Setup

```bash
git clone https://github.com/freezeframe/freezeframe
cd freezeframe
uv sync --group dev --group docs
```

## Running checks

All of these must pass before a PR can be merged.

```bash
uv run pytest                        # tests
uv run ruff check src tests          # lint
uv run ruff format --check src tests # formatting
uv run ty check src                  # type checking
```

## Serving docs locally

```bash
uv run mkdocs serve
```

Then open `http://127.0.0.1:8000`.

## PR checklist

- [ ] Tests added or updated for the change
- [ ] `ruff check`, `ruff format`, `ty check` all pass
- [ ] Docstrings updated if public API changed
- [ ] `CHANGELOG.md` updated under `[Unreleased]`
- [ ] Docs updated if behaviour changed

## Commit style

Use short imperative subject lines:

```
add FrozenFrameMeta metaclass
fix SchemaValidationError message for nullable columns
docs: add null handling guide
```

## Code style

- Formatter: `ruff format` (88-char lines, double quotes)
- Type annotations on all public functions
- Numpy-style docstrings for public API
- No `apply` or untyped escape hatches — stay within the type-safe surface

## Project phases

The [roadmap](https://github.com/freezeframe/freezeframe/blob/main/roadmap.md)
describes what is planned for each release. If you want to work on a Phase 2
or 3 feature while Phase 1 is still in progress, open an issue to coordinate.
