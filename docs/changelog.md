# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project uses [semantic versioning](https://semver.org/).

---

## [Unreleased]

### Added
- Initial project structure (`src/` layout, `pyproject.toml`, `mkdocs.yml`)
- Package skeleton: `FrozenFrame`, `field`, `FrozenSeries`, `FrozenFrameError`, `SchemaValidationError` stubs
- `roadmap.md` and `motivations.md` project documentation
- MkDocs Material documentation site with full guide, API reference, and contributing pages
- `ruff` (linting + formatting) and `ty` (type checking) configured in `pyproject.toml`
- **`column.py`** — Phase 1 implementation complete:
  - `_PYTHON_TO_ARROW` default type map covering `bool`, `int`, `float`, `str`, `bytes`, `datetime.datetime`, `datetime.date`, `datetime.timedelta`, `decimal.Decimal`
  - `register_type()` for global third-party type registration
  - `field()` for optional per-column Arrow type overrides and documentation
  - `resolve_arrow_type()` internal resolver handling bare types, `T | None` unions, `Optional[T]`, and `field(arrow_type=...)` overrides
- 28 tests for `column.py` covering all type mappings, nullable unions, overrides, error cases, `field()`, and `register_type()`
- **`schema.py`** — Phase 1 implementation complete:
  - `build_schema(annotations, defaults)` builds a `pa.Schema` from a class's `__annotations__` and any `field()` defaults, respecting `arrow_type` overrides and `T | None` nullability
  - `validate(schema, batch)` checks a `pa.RecordBatch` for extra columns, missing columns, type mismatches, and nullability violations — each with a descriptive error message naming the offending column
- 21 tests for `schema.py` covering basic types, nullability, `field()` overrides, column ordering, error cases, and passing validation
- **`frame.py`** — Phase 1 implementation complete:
  - `FrozenFrameMeta` metaclass with `@dataclass_transform()` — compiles `__annotations__` into `cls.__schema__` at class creation time, resolves forward references via `get_type_hints`, collects `field()` defaults
  - `FrozenFrame` base class with `from_dict`, `from_arrow`, and `_from_batch` (internal fast-path) constructors
  - Mutation guards: `__setattr__`, `__delattr__`, `__setitem__`, `__delitem__` all raise `FrozenFrameError`
  - Column access via `df["col"]` and `df.col` (returns `pa.Array`; upgrades to `FrozenSeries` in next step)
  - `__hash__` — SHA-256 of Arrow IPC serialisation, cached after first computation
  - `__eq__`, `__len__`, `__iter__` (row-wise dicts), `__repr__`
- 45 tests for `frame.py` covering metaclass schema compilation, construction, mutation guards, column access, iteration, equality, hashing, and repr
- **`__init__.py`** — public API wired up: `FrozenFrame`, `field`, `register_type`, `FrozenFrameError`, `SchemaValidationError` all importable directly from `freezeframe`

---

<!-- Add new releases above this line -->
[Unreleased]: https://github.com/freezeframe/freezeframe/compare/HEAD
