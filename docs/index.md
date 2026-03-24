# freezeFrame

**Immutable, schema-typed DataFrames backed by Apache Arrow.**

`freezeFrame` is a Python 3.13+ DataFrame library for production data pipelines where correctness matters. It enforces immutability at the storage layer, validates schemas at construction time, and exposes typed column access that works with static analysis tools out of the box.

```python
from freezeframe import FrozenFrame

class UserMetrics(FrozenFrame):
    user_id: int
    name:    str
    score:   float
    active:  bool | None  # nullable — explicit opt-in

df = UserMetrics.from_dict({
    "user_id": [1, 2, 3],
    "name":    ["alice", "bob", "carol"],
    "score":   [9.1, 7.4, 8.8],
    "active":  [True, False, None],
})

df.score           # -> FrozenSeries[float]
df["score"] = []   # raises FrozenFrameError — mutations are not possible

top = df.filter(df.score > 8.0)  # -> UserMetrics
cache = {df: result}             # works — FrozenFrame is hashable
```

---

## Why freezeFrame?

Pandas is excellent for exploration. In production pipelines, its mutability is a liability.

=== "The problem"

    ```python
    def compute_score(df: pd.DataFrame) -> pd.DataFrame:
        df["score"] = df["value"] / df["total"]  # silently mutates the caller's frame
        return df
    ```

    Any function receiving a Pandas DataFrame can modify it in place.
    There is no language-level protection. Schema mismatches are discovered
    at runtime, deep in a pipeline, far from where the bad data entered.

=== "The solution"

    ```python
    class Metrics(FrozenFrame):
        value: float
        total: float

    def compute_score(df: Metrics) -> Metrics:
        df["score"] = ...  # raises FrozenFrameError immediately
    ```

    Mutations are physically impossible. Schemas are declared once,
    checked statically, and validated at runtime on construction.

---

## Core guarantees

| Guarantee | Mechanism |
|---|---|
| **Immutability** | Apache Arrow read-only buffers + Python mutation guards |
| **Schema typing** | Native type annotations + `dataclass_transform` (PEP 681) |
| **Nullable safety** | `T \| None` is explicit — never silently coerced |
| **Hashability** | `__hash__` based on schema + content fingerprint |
| **Zero-copy interop** | Arrow C Stream protocol — Polars, DuckDB, Pandas 3.x |

---

## Installation

```bash
pip install freezeframe
```

See [Installation](getting-started/installation.md) for optional extras and dev setup.

---

## Status

!!! warning "Pre-release"
    freezeFrame is under active development. The API shown here reflects the
    target design. See the [Changelog](changelog.md) for what is implemented.
