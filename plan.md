# freezeFrame — Implementation Plan

This document is a living reference for picking up development across sessions.
It summarises what has been built, the decisions made along the way, and what
comes next.  Read this before starting any new session.

---

## Project Summary

`freezeFrame` is an immutable, schema-typed DataFrame library for Python 3.13+
backed by Apache Arrow.  Users declare schemas as plain Python classes with
native type annotations.  Mutations are physically impossible (Arrow read-only
buffers + Python guards).  Frames are hashable, schema-validated at construction,
and zero-copy interoperable with Polars, DuckDB, and Pandas 3.x.

**Key design decisions (already locked in):**
- Schema via native Python type annotations — `user_id: int`, not `Column[int]`
- `T | None` for nullable columns — explicit, never silently coerced
- `field()` for Arrow type overrides / column docs only — never required for basic use
- `FrozenFrameMeta` metaclass with `@dataclass_transform()` compiles schema at class creation
- Internal store: `pa.RecordBatch` (Arrow read-only buffers)
- `_from_batch(batch)` private classmethod for zero-validation internal construction (used by transforms)
- `FrozenSeries` comparison operators return `pa.BooleanArray` (masks) for filter expressions
- `uv` for package management, `ruff` for lint/format, `ty` for type checking, `pytest` for tests
- `src/` layout, `hatchling` build backend, MkDocs Material for docs

---

## Phase 1 — Core (COMPLETE)

All Phase 1 checklist items are done.  129 tests passing.  ruff and ty clean.

### What was built

| File | Status | Description |
|---|---|---|
| `src/freezeframe/__init__.py` | ✅ Done | Public API exports |
| `src/freezeframe/exceptions.py` | ✅ Done | `FrozenFrameError`, `SchemaValidationError` |
| `src/freezeframe/column.py` | ✅ Done | `field()`, `register_type()`, `resolve_arrow_type()`, `_PYTHON_TO_ARROW` |
| `src/freezeframe/schema.py` | ✅ Done | `build_schema()`, `validate()` |
| `src/freezeframe/frame.py` | ✅ Done | `FrozenFrameMeta`, `FrozenFrame` |
| `src/freezeframe/series.py` | ✅ Done | `FrozenSeries[T]` |
| `src/freezeframe/_typing.py` | stub | Reserved for Phase 3 TypeVarTuple helpers |
| `src/freezeframe/ops/` | stubs | All Phase 2 |
| `src/freezeframe/interop/` | stubs | All Phase 3 |
| `tests/test_column.py` | ✅ Done | 28 tests |
| `tests/test_schema.py` | ✅ Done | 21 tests |
| `tests/test_frame.py` | ✅ Done | 45 tests |
| `tests/test_series.py` | ✅ Done | 45 tests |

### Public API (importable from `freezeframe`)

```python
from freezeframe import (
    FrozenFrame,          # base class — subclass to declare a schema
    FrozenSeries,         # typed column accessor returned by frame[col] / frame.col
    FrozenFrameError,     # raised on mutation attempts
    SchemaValidationError,# raised on schema/data mismatch at construction
    field,                # optional per-column metadata (arrow_type, description)
    register_type,        # register a global Python→Arrow type mapping
)
```

### Key implementation details to remember

**`column.py`**
- `_PYTHON_TO_ARROW` maps: `bool`, `int`, `float`, `str`, `bytes`,
  `datetime.datetime`, `datetime.date`, `datetime.timedelta`, `decimal.Decimal`
- `resolve_arrow_type(annotation, override)` handles bare types, `T | None`
  (modern union), `Optional[T]` (typing module), and `field(arrow_type=...)` overrides
- Multi-member unions (e.g. `int | str`) raise `TypeError` — only `T | None` supported

**`schema.py`**
- `build_schema(annotations, defaults)` — takes `cls.__annotations__` and the
  class `__dict__` entries that are `field()` instances
- `validate(schema, batch)` — checks extra cols, missing cols, type match,
  nullability; raises `SchemaValidationError` with the offending column named

**`frame.py`**
- `FrozenFrameMeta.__new__` skips `FrozenFrame` itself (no `FrozenFrameMeta`
  in bases), uses `typing.get_type_hints(cls)` for forward-ref resolution,
  falls back to raw `__annotations__` on failure
- `FrozenFrame._from_batch(batch)` — zero-validation internal constructor;
  ALL transform operations must use this to avoid redundant validation overhead
- `__setattr__` raises unconditionally; internal writes use `object.__setattr__`
- `__hash__` serialises to Arrow IPC bytes → SHA-256 → int; cached in
  `_hash_cache` via `object.__setattr__`
- `_batch: pa.RecordBatch` and `_hash_cache: int` are declared as class-level
  annotations so `ty` can resolve them (they're set at runtime via
  `object.__setattr__`, not at class definition time)

**`series.py`**
- Comparison operators (`==`, `!=`, `<`, `<=`, `>`, `>=`) use `pyarrow.compute`
  and return `pa.Array` (boolean) — these are filter masks, not Python bools
- `__hash__ = None` — explicitly unhashable because `__eq__` is element-wise
- `_array: pa.Array` declared at class level for the same `ty` reason as above
- `pyarrow.compute` functions are suppressed with `# type: ignore[attr-defined]`
  because `ty` (alpha) lacks complete pyarrow stubs — this is expected

---

## Phase 2 — Functional API (TODO)

**Target version:** `0.2.x`

The goal is a complete set of functional transforms.  All operations return
new `FrozenFrame` instances — the original is never modified.

### Schema-preserving operations (return same concrete subtype)

- [ ] **`filter(mask)`** — `src/freezeframe/ops/filter.py`
  - Accepts a `pa.BooleanArray` (from `FrozenSeries` comparison) or a `FrozenSeries[bool]`
  - Uses `pa.RecordBatch.filter(mask)`
  - Returns `cls._from_batch(filtered_batch)`
  - Should be a method on `FrozenFrame`, delegating to the ops module

- [ ] **`sort(*columns, descending=False)`** — `src/freezeframe/ops/sort.py`
  - Accepts one or more column names; `descending` can be `bool` or `list[bool]`
  - Uses `pyarrow.compute.sort_indices` then `pa.RecordBatch.take`

- [ ] **`slice(start, stop)`** — `src/freezeframe/ops/sort.py` or inline
  - Simple row slice; uses `batch.slice(offset, length)`

### Schema-altering operations (return `FrozenFrame` with new schema)

- [ ] **`select(*columns)`** — `src/freezeframe/ops/select.py`
  - Returns a new `FrozenFrame` containing only the specified columns
  - Schema is rebuilt from the selected fields
  - Full static typing of the return type is a Phase 3 goal (TypeVarTuple)

- [ ] **`rename(mapping: dict[str, str])`** — `src/freezeframe/ops/rename.py`
  - Returns a new `FrozenFrame` with renamed columns
  - Validates that all keys exist and no target name collides

- [ ] **`drop(*columns)`** — can live in `select.py` or `with_columns.py`
  - Inverse of `select`; removes named columns

- [ ] **`with_column(name, data)`** — `src/freezeframe/ops/with_columns.py`
  - Adds a new column from a `pa.Array`, `FrozenSeries`, or Python list
  - Raises if `name` already exists (use `replace_column` for updates)

- [ ] **`join(other, on, how="inner")`** — `src/freezeframe/ops/join.py`
  - Runtime schema inference only in Phase 2 (typed overloads deferred to Phase 3)
  - Use `pyarrow` join via converting to `pa.Table` temporarily

### Expression system (needed for `filter`)

The current `FrozenSeries` comparison operators already return `pa.BooleanArray`.
Phase 2 needs `FrozenFrame.filter` to accept that directly.  No separate
expression AST is needed for Phase 2 — raw `pa.BooleanArray` is enough.

Consider adding `&` and `|` operators to combine masks:
```python
mask = (df.score > 8.0) & (df.active == True)
top = df.filter(mask)
```
These operate on `pa.BooleanArray` directly via `pyarrow.compute.and_` / `or_`.

### `map_column`

- [ ] **`map_column(col, fn)`** — typed UDF replacement for `apply`
  - `fn` must be typed: `Callable[[FrozenSeries[T]], FrozenSeries[U]]`
  - Returns a new frame with the column replaced

### Testing approach for Phase 2

Each transform needs tests for:
1. The happy path — correct output
2. Schema preservation (same type returned for schema-preserving ops)
3. Immutability of the original frame (unchanged after transform)
4. Edge cases — empty frame, single row, all-null column

---

## Phase 3 — Ecosystem & Static Typing (TODO)

**Target version:** `0.3.x`

- [ ] `from_polars` / `to_polars` — zero-copy via `__arrow_c_stream__`
- [ ] `from_pandas` / `to_pandas` — Pandas 3.x Arrow-backed
- [ ] `from_duckdb` / `to_duckdb` — via Arrow relation
- [ ] `__arrow_c_stream__` protocol on `FrozenFrame`
- [ ] TypeVarTuple-typed `select()` overloads
- [ ] `from_parquet` / `to_parquet`, `from_ipc` / `to_ipc`
- [ ] `map_column` typed overloads

---

## Phase 4 — Production & Community (TODO)

**Target version:** `1.0.0`

- [ ] Stable public API with deprecation policy
- [ ] Full MkDocs documentation site (content currently scaffolded in `docs/`)
- [ ] Performance benchmarks vs. static-frame and Pandas immutable patterns
- [ ] `CONTRIBUTING.md` fleshed out with issue/PR templates
- [ ] Automated releases via GitHub Actions
- [ ] PyPI publish workflow

---

## Development Workflow

```bash
# install all dev + docs deps
uv sync --group dev --group docs

# run everything
uv run pytest                        # 129 tests (grows with each phase)
uv run ruff check src tests          # lint
uv run ruff format src tests         # format
uv run ty check src                  # type check
uv run mkdocs serve                  # docs preview at localhost:8000
```

**Before merging anything:** all four commands above must pass cleanly.

## Known ty suppressions

`pyarrow.compute` functions in `series.py` are suppressed with
`# type: ignore[attr-defined]` on each call site.  This is intentional —
`ty` (currently in alpha) lacks complete stubs for `pyarrow.compute`.
Do not remove these suppressions until `ty` ships proper pyarrow support.
