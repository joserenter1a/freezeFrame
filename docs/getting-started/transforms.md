# Functional Transforms

All transforms return a **new** `FrozenFrame`. The original is never modified.

---

## Schema-preserving operations

These operations change the rows but not the schema. They return the same
concrete subtype — not a generic `FrozenFrame` base class.

### filter

```python
top: UserMetrics = df.filter(df.score > 8.0)
active: UserMetrics = df.filter(df.active == True)
```

Filter expressions are built from `FrozenSeries` comparisons. They produce a
lazy predicate evaluated against the underlying Arrow array — not a Python list
of booleans.

### sort

```python
ranked: UserMetrics = df.sort("score", descending=True)
multi:  UserMetrics = df.sort("active", "score", descending=[False, True])
```

### slice

```python
first_ten: UserMetrics = df.slice(0, 10)
```

---

## Schema-altering operations

These operations change the columns and therefore the schema. They return a
`FrozenFrame` instance (full static typing of the output schema is a Phase 3
goal — see the [roadmap](../contributing.md)).

### select

```python
slim = df.select("user_id", "score")
```

### rename

```python
renamed = df.rename({"user_id": "id", "score": "metric"})
```

### drop

```python
without_active = df.drop("active")
```

### with_column

Add a new column from a `FrozenSeries` or Arrow array:

```python
import pyarrow as pa
labels = pa.array(["high", "low", "high"])
labelled = df.with_column("label", labels)
```

### join

```python
result = df.join(other_df, on="user_id", how="inner")
```

---

## map_column

`map_column` applies a typed function to a single column and returns a new
frame with that column replaced. It is the intended replacement for Pandas'
`apply` — and unlike `apply`, the function signature is type-checked:

```python
def normalize(s: FrozenSeries[float]) -> FrozenSeries[float]:
    ...

normed: UserMetrics = df.map_column("score", normalize)
```

!!! note
    There is no `apply` or `map` escape hatch in `freezeFrame`. `map_column`
    with a typed signature is the only supported UDF path, by design.
