# freezeFrame

**Immutable, schema-typed DataFrames backed by Apache Arrow.**

`freezeFrame` is a Python 3.13+ DataFrame library for production data pipelines where correctness matters. Unlike Pandas, which is mutable by default and schema-opaque, `freezeFrame` enforces immutability at the storage layer, validates schemas at construction time, and exposes typed column access that works with static analysis tools out of the box.

```python
from freezeframe import FrozenFrame

class UserMetrics(FrozenFrame):
    user_id: int
    name:    str
    score:   float
    active:  bool | None  # nullable — explicit opt-in via union with None

df = UserMetrics.from_dict({
    "user_id": [1, 2, 3],
    "name":    ["alice", "bob", "carol"],
    "score":   [9.1, 7.4, 8.8],
    "active":  [True, False, None],
})

df.score          # -> FrozenSeries[float]  (IDE-completable, type-checked)
df["score"]       # -> FrozenSeries[float]

df["score"] = []  # raises FrozenFrameError — mutations are not possible

top = df.filter(df.score > 8.0)   # -> UserMetrics (same schema, new frame)
ids = df.select("user_id", "score")  # -> FrozenFrame (schema-altering)

cache = {df: result}   # works — FrozenFrame is hashable
```

Need to override the default Arrow type or document a column? Use `field()`:

```python
import pyarrow as pa
from freezeframe import FrozenFrame, field

class Events(FrozenFrame):
    user_id: int
    score:   float = field(arrow_type=pa.float32())
    label:   str   = field(description="human-readable event label")
```

> **Status:** Pre-release. The API shown above reflects the target design. Implementation is in progress — see the [roadmap](roadmap.md) for what is and isn't built yet.

---

## Why freezeFrame?

Pandas is excellent for exploration. It is a liability in production.

- **Silent mutation** — any function receiving a DataFrame can modify it in place. There is no language-level protection.
- **Schema opacity** — `df["score"]` returns `Any`. Type checkers, IDEs, and function signatures can't reason about column types or shapes.
- **No hashability** — mutable objects can't be used as dict keys, stored in sets, or memoized.

`freezeFrame` addresses all three:

| Guarantee | Mechanism |
|---|---|
| Immutability | Apache Arrow read-only buffers + Python mutation guards |
| Schema typing | class definitions + `dataclass_transform` (PEP 681) |
| Hashability | `__hash__` based on schema + content fingerprint |

For a deeper treatment, see [motivations.md](motivations.md).

---

## Architecture

### Package layout (src layout)

```
src/freezeframe/
├── __init__.py          # public API re-exports
├── frame.py             # FrozenFrame base class + FrozenFrameMeta metaclass
├── column.py            # Column[T] descriptor + Python→Arrow type mapping
├── series.py            # FrozenSeries[T] — typed column accessor
├── schema.py            # schema validation, null enforcement
├── exceptions.py        # FrozenFrameError, SchemaValidationError
├── _typing.py           # internal TypeVarTuple / PEP 695 helpers
├── ops/
│   ├── filter.py        # filter()       — schema-preserving
│   ├── sort.py          # sort()         — schema-preserving
│   ├── select.py        # select()       — schema-altering
│   ├── rename.py        # rename()       — schema-altering
│   ├── with_columns.py  # with_column(), drop() — schema-altering
│   └── join.py          # join()         — schema-altering
└── interop/
    ├── arrow.py         # from_arrow / to_arrow
    ├── pandas.py        # from_pandas / to_pandas  (Pandas 3.x)
    └── polars.py        # from_polars / to_polars  (zero-copy)
```

### Design principles

**Backend: Apache Arrow.** `pyarrow.RecordBatch` is the internal store. Arrow buffers are allocated read-only at the C layer — there is no mutation to defend against at the Python level because it isn't physically possible underneath.

**Schema as a class definition.** `dataclass_transform` (PEP 681) teaches mypy and pyright about bare type annotation fields the same way it works for dataclasses and Pydantic models. Schemas are defined once using native Python type hints, checked statically, and validated at runtime on construction. `field()` is available for the rare cases that need Arrow type overrides or column documentation.

**Nullability is explicit.** `Column[int]` means non-nullable. `Column[int | None]` means nullable. This is enforced at construction — not silently coerced, not inferred.

**Functional transforms.** Schema-preserving operations (`filter`, `sort`, `slice`) return the same concrete subtype. Schema-altering operations (`select`, `rename`, `join`) return a new `FrozenFrame`. There is no `apply` or `map` escape hatch; typed `map_column(col, fn)` is the safe alternative.

**Zero-copy interop.** In Phase 3, Polars, DuckDB, and Pandas 3.x frames can round-trip through `freezeFrame` via the Arrow C Stream protocol (`__arrow_c_stream__`) with no data copies.

---

## Installation

```bash
# core (Apache Arrow backend only)
pip install freezeframe

# with optional ecosystem extras
pip install "freezeframe[pandas]"
pip install "freezeframe[polars]"
pip install "freezeframe[duckdb]"
pip install "freezeframe[all]"
```

Requires Python 3.13+.

---

## Development

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

```bash
# clone and install with dev dependencies
git clone https://github.com/<org>/freezeframe
cd freezeframe
uv sync --group dev

# run tests
uv run pytest

# lint
uv run ruff check src tests

# format
uv run ruff format src tests

# type check
uv run ty check src
```

---

## Roadmap

| Phase | Version | Focus |
|---|---|---|
| 1 | `0.1.x` | Core: `FrozenFrame`, `Column[T]`, schema validation, mutation guards, `__hash__` |
| 2 | `0.2.x` | Functional API: `filter`, `sort`, `select`, `join`, expression system |
| 3 | `0.3.x` | Ecosystem: Polars/Pandas/DuckDB interop, TypeVarTuple-typed `select`, pyright stubs |
| 4 | `1.0.0` | Stable API, docs site, benchmarks, community infrastructure |

See [roadmap.md](roadmap.md) for full detail.

---

## Contributing

`freezeFrame` is open source (MIT). Contributions are welcome.

- Open an issue before starting significant work so we can align on the approach
- PRs must include tests, pass `ruff check`, `ty check`, and `pytest`
- Update `CHANGELOG.md` with your change

---

## License

MIT
