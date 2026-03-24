# Installation

## Requirements

- Python **3.13** or later
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

---

## Core install

```bash
pip install freezeframe
```

The only runtime dependency is `pyarrow>=17.0`.

---

## Optional extras

Install ecosystem integrations as needed:

```bash
# Pandas 3.x interop
pip install "freezeframe[pandas]"

# Polars interop (zero-copy via Arrow C Stream)
pip install "freezeframe[polars]"

# DuckDB interop
pip install "freezeframe[duckdb]"

# Everything
pip install "freezeframe[all]"
```

---

## Development install

Clone the repo and install with dev and docs dependencies using uv:

```bash
git clone https://github.com/freezeframe/freezeframe
cd freezeframe
uv sync --group dev --group docs
```

### Useful commands

| Task | Command |
|---|---|
| Run tests | `uv run pytest` |
| Lint | `uv run ruff check src tests` |
| Format | `uv run ruff format src tests` |
| Type check | `uv run ty check src` |
| Serve docs locally | `uv run mkdocs serve` |
| Build docs | `uv run mkdocs build` |
