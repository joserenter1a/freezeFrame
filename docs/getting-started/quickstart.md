# Quickstart

## Define a schema

Schemas are declared as Python classes. Use native type annotations — no wrapper types needed.
Nullable columns use `T | None`.

```python
from freezeframe import FrozenFrame

class UserMetrics(FrozenFrame):
    user_id: int
    name:    str
    score:   float
    active:  bool | None  # nullable
```

## Construct a frame

```python
df = UserMetrics.from_dict({
    "user_id": [1, 2, 3],
    "name":    ["alice", "bob", "carol"],
    "score":   [9.1, 7.4, 8.8],
    "active":  [True, False, None],
})
```

`from_dict` validates the data against the declared schema at construction time.
A `SchemaValidationError` is raised immediately if the data doesn't match.

## Access columns

Column access returns a `FrozenSeries[T]`. The type is known statically — your
IDE will autocomplete column names and type checkers will catch mismatches.

```python
df.score          # FrozenSeries[float]
df["user_id"]     # FrozenSeries[int]

len(df)           # 3
list(df.name)     # ["alice", "bob", "carol"]
```

## Mutations are rejected

```python
df["score"] = [1.0, 2.0, 3.0]  # raises FrozenFrameError
df.score = ...                  # raises FrozenFrameError
del df.name                     # raises FrozenFrameError
```

## Filter rows

`filter` is schema-preserving — it returns the same concrete type (`UserMetrics`),
not a generic base class.

```python
top: UserMetrics = df.filter(df.score > 8.0)
```

## Select columns

`select` is schema-altering — it returns a `FrozenFrame` with the new schema.

```python
slim = df.select("user_id", "score")
```

## Use as a dict key

Because `FrozenFrame` implements `__hash__`, frames can be used anywhere a
hashable object is required.

```python
import functools

@functools.lru_cache
def expensive_computation(df: UserMetrics) -> float:
    ...
```

## Override Arrow types with `field()`

For the rare cases where you need a specific Arrow type (e.g. `float32` instead
of the default `float64`), use `field()`:

```python
import pyarrow as pa
from freezeframe import FrozenFrame, field

class CompactMetrics(FrozenFrame):
    user_id: int
    score:   float = field(arrow_type=pa.float32())
    label:   str   = field(description="event classification label")
```

`field()` is never required for basic use.
