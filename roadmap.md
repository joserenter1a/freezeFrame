# freezeFrame — Project Roadmap & Architecture

## Vision

`freezeFrame` is a modern, immutable DataFrame library for Python 3.13+. Unlike Pandas, which is mutable by default and schema-opaque, `freezeFrame` enforces immutability at the storage layer (Apache Arrow), validates schemas at construction time, and exposes typed column access that works with mypy/pyright out of the box. It targets production data pipelines where correctness, idempotence, and functional-style programming matter.

**Target audience:** Data engineers, ML engineers, and backend teams building reliable, tested data pipelines — not exploratory notebook users.

---

## Package Name

`freezeframe` — installable as:

```bash
pip install freezeframe
```

PyPI: `https://pypi.org/project/freezeframe`

---

## Architecture Overview

```
freezeframe/
├── __init__.py              # Public API re-exports
├── frame.py                 # FrozenFrame base class + FrozenFrameMeta metaclass
├── column.py                # field() + Python→Arrow type mapping
├── series.py                # FrozenSeries[T] — typed column accessor
├── schema.py                # Schema validation logic, null handling
├── ops/
│   ├── __init__.py
│   ├── filter.py            # filter() — schema-preserving
│   ├── sort.py              # sort() — schema-preserving
│   ├── select.py            # select() — schema-altering
│   ├── rename.py            # rename() — schema-altering
│   ├── join.py              # join() — schema-altering (Phase 2)
│   └── with_columns.py      # with_columns() — schema-altering (Phase 2)
├── interop/
│   ├── __init__.py
│   ├── arrow.py             # from_arrow / to_arrow
│   ├── pandas.py            # from_pandas / to_pandas
│   └── polars.py            # from_polars / to_polars
├── exceptions.py            # FrozenFrameError, SchemaValidationError, etc.
└── _typing.py               # Internal TypeVarTuple / PEP 695 helpers
```

```
tests/
├── test_frame.py
├── test_column.py
├── test_schema.py
├── test_ops/
└── test_interop/

docs/
├── index.md
├── quickstart.md
├── schema_definition.md
├── functional_api.md
└── interop.md
```

---

## Core Design Decisions

### 1. Backend: Apache Arrow (pyarrow)

Arrow `RecordBatch` is the internal store. Arrow buffers are allocated read-only — mutations at the C layer raise `ArrowNotImplementedError` before Python even gets involved. This gives immutability largely for free.

### 2. Schema as a Class Definition

Uses `dataclass_transform` (PEP 681) so type checkers understand the metaclass machinery — the same mechanism Pydantic and attrs use. Schema is declared with **native Python type annotations** — no wrapper type required:

```python
from freezeframe import FrozenFrame, field
import pyarrow as pa

class UserMetrics(FrozenFrame):
    user_id: int
    name:    str
    score:   float
    active:  bool | None  # nullable — explicit opt-in via union with None
```

The metaclass reads `__annotations__` at class creation and maps Python types to Arrow types. `field()` is available only when you need to override the default Arrow type or attach metadata — it is never required:

```python
class Events(FrozenFrame):
    user_id: int
    score:   float = field(arrow_type=pa.float32())   # override default float64
    label:   str   = field(description="event label")
```

Nullable columns use `T | None` (enforced at construction, not silently coerced).

### 3. Three-Layer Immutability

1. **Storage** — Arrow read-only buffers
2. **Python API** — `__setitem__`, `__setattr__`, `__delitem__` unconditionally raise `FrozenFrameError`
3. **Hashability** — `__hash__` is implemented (schema fingerprint + content hash), making frames usable as dict keys and set members

### 4. Typed Column Access

Because the schema is declared with native type annotations, type checkers already know the type of every attribute — no plugin needed for the basic case:

```python
df.user_id        # -> FrozenSeries[int]
df["user_id"]     # -> FrozenSeries[int]
```

`FrozenSeries[T]` wraps an Arrow `ChunkedArray` and supports typed iteration, `__len__`, `__getitem__`, and comparison operators for use in filter expressions.

### 5. Functional Transform API

Schema-preserving operations return the same concrete type:
```python
filtered: UserMetrics = df.filter(df.score > 8.0)
sorted_df: UserMetrics = df.sort("score", descending=True)
```

Schema-altering operations return a generic `FrozenFrame` initially (TypeVarTuple-typed overloads added in Phase 3):
```python
selected = df.select("user_id", "score")  # FrozenFrame at runtime; typed in Phase 3
```

---

## Phases

### Phase 1 — Core MVP

**Goal:** A usable, publishable `0.1.x` on PyPI.

- [ ] `FrozenFrameMeta` metaclass with `dataclass_transform` support
- [ ] Python type → Arrow type mapping (built-in type map + `register_type()` for extensions)
- [ ] `field()` for optional Arrow type overrides and column documentation
- [ ] Schema validation at `__init__`, `from_dict`, `from_arrow`, `from_records`
- [ ] `__setitem__` / `__setattr__` / `__delitem__` mutation guards
- [ ] `__hash__` and `__eq__` (schema + content fingerprint)
- [ ] `__repr__`, `__len__`, `__iter__` (yields row-wise dicts)
- [ ] `FrozenSeries[T]` with typed access and comparison operators
- [ ] `FrozenFrameError`, `SchemaValidationError` exceptions
- [ ] `validate=False` fast-path for `from_arrow` (hot paths)
- [ ] `pyproject.toml` with proper metadata, classifiers, and PyPI trove classifiers
- [ ] CI: GitHub Actions — lint (ruff), type check (ty), test (pytest)
- [ ] `CONTRIBUTING.md`, `CHANGELOG.md`, `LICENSE` (MIT)

**Milestone:** `pip install freezeframe` works; a `UserMetrics` frame rejects mutations and passes schema validation.

---

### Phase 2 — Functional API

**Goal:** `0.2.x` — a complete, useful functional transform API.

- [ ] `filter(expr)` — schema-preserving, returns same concrete type
- [ ] `sort(*columns, descending=False)` — schema-preserving
- [ ] `rename(mapping: dict[str, str])` — returns new schema
- [ ] `select(*columns)` — runtime-validated schema-altering
- [ ] `with_column(name, series)` — adds a column, returns new schema
- [ ] `drop(*columns)` — removes columns, returns new schema
- [ ] `join(other, on, how="inner")` — runtime-only schema inference (typed overloads deferred)
- [ ] `slice(start, stop)` — schema-preserving
- [ ] Expression system for filter: `df.score > 8.0` produces a lazy predicate, not a boolean array

**Milestone:** End-to-end pipeline — read, filter, select, join, write — using only `freezeframe`.

---

### Phase 3 — Ecosystem & Static Typing

**Goal:** `0.3.x` — zero-copy interop with the Arrow ecosystem + advanced static analysis.

- [ ] `from_polars` / `to_polars` via `__arrow_c_stream__` (zero-copy)
- [ ] `from_pandas` / `to_pandas` (Pandas 3.x Arrow-backed)
- [ ] `to_duckdb` relation / `from_duckdb`
- [ ] TypeVarTuple-typed `select()` overloads (returns `FrozenFrame` narrowed to selected columns)
- [ ] pyright plugin stubs for schema-altering ops that `dataclass_transform` can't cover
- [ ] `map_column(col, fn: Callable[[T], U])` — typed UDF replacement (no `apply` escape hatch)
- [ ] Serialization: `to_parquet` / `from_parquet`, `to_ipc` / `from_ipc` via Arrow

**Milestone:** A Polars or Pandas frame can round-trip through `freezeframe` with zero copies; pyright reports column type errors.

---

### Phase 4 — Production & Community

**Goal:** `1.0.0` — stable, well-documented, community-ready.

- [ ] Stable public API with deprecation policy
- [ ] Full documentation site (MkDocs + Material theme)
- [ ] Performance benchmarks vs. static-frame and Pandas immutable patterns
- [ ] `freezeframe[all]` extras grouping: `freezeframe[polars]`, `freezeframe[pandas]`, `freezeframe[duckdb]`
- [ ] Type stubs published as `freezeframe-stubs` if needed
- [ ] Issue templates, PR templates, contributor onboarding guide
- [ ] GitHub Discussions enabled for community Q&A
- [ ] Semantic versioning + automated releases via `release-please` or `bumpversion`

---

## PyPI Publishing Setup

`pyproject.toml` will use `[build-system]` with `hatchling` (modern, no `setup.py`):

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "freezeframe"
version = "0.1.0"
description = "Immutable, schema-typed DataFrames backed by Apache Arrow"
readme = "README.md"
requires-python = ">=3.13"
license = { text = "MIT" }
keywords = ["dataframe", "immutable", "arrow", "typed", "functional"]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "Intended Audience :: Science/Research",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3.13",
    "Topic :: Scientific/Engineering",
    "Typing :: Typed",
]
dependencies = ["pyarrow>=17.0"]

[project.optional-dependencies]
pandas = ["pandas>=3.0"]
polars = ["polars>=1.0"]
duckdb = ["duckdb>=1.0"]
all = ["pandas>=3.0", "polars>=1.0", "duckdb>=1.0"]
dev = ["pytest", "pyright", "ruff", "hatch"]
```

---

## Open Source Contribution Model

- **License:** MIT
- **Branching:** `main` is always releasable; feature branches merged via PR
- **Versioning:** Semantic versioning (`MAJOR.MINOR.PATCH`)
- **Issues:** Bug reports use a structured template; feature requests use a separate template with a "motivation" section
- **PRs:** Must include tests, pass CI (ruff + ty + pytest), and update `CHANGELOG.md`
- **Governance:** Maintainer-led initially; `CODEOWNERS` file designates reviewers by module

---

## Hard Problems (Deferred, Not Forgotten)

| Problem | Plan |
|---|---|
| `select()` static typing | TypeVarTuple overloads in Phase 3; accept runtime-only in Phase 1–2 |
| Null handling | `T \| None` annotation is explicit; no silent coercion |
| UDFs / `apply` | No `apply`; use `map_column(col, fn)` with typed signature only |
| Construction cost | `validate=False` fast-path; validation on by default at public boundaries |
| `join` typing | Runtime schema inference first; typed overloads after TypeVarTuple lands broadly |

---

## Differentiation Summary

| | static-frame | freezeFrame |
|---|---|---|
| Backend | NumPy / Pandas 1.x | Apache Arrow (native) |
| Immutability | Python-level copy | Arrow read-only buffers + Python |
| Type system | Pre-PEP 646 | `dataclass_transform`, TypeVarTuple, PEP 695 |
| Interop | Pandas only | Arrow ecosystem (Polars, DuckDB, Pandas 3.x) |
| `__hash__` | No | Yes |
| Schema syntax | Custom wrapper type | Native Python type annotations |
| Python version | 3.8+ | 3.13+ |
