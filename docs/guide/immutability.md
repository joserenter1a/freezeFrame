# Immutability

## Three layers

`freezeFrame` enforces immutability at three independent levels. Each layer
catches a different class of mutation attempt.

### 1. Storage layer — Apache Arrow read-only buffers

The internal store is a `pyarrow.RecordBatch`. Arrow allocates its memory
buffers as read-only at the C layer. Any attempt to mutate the underlying
data raises `ArrowNotImplementedError` before Python is even involved.

This is the most important layer. It means immutability is a property of the
data, not just a convention enforced by the Python API.

### 2. Python API layer — mutation guards

`FrozenFrame` overrides `__setitem__`, `__setattr__`, and `__delitem__` to
raise `FrozenFrameError` unconditionally:

```python
df["score"] = [1.0, 2.0]  # FrozenFrameError
df.score = ...             # FrozenFrameError
del df.name                # FrozenFrameError
```

### 3. Hashability — stable identity

Because `FrozenFrame` implements `__hash__`, a frame has a stable identity.
This is the semantic complement to immutability: something that can be hashed
is, by definition, something that cannot change.

```python
seen = set()
seen.add(df)          # works

cache = {df: result}  # works
```

The hash is computed from the Arrow schema and a content fingerprint of the
underlying buffers.

---

## What immutability enables

**Functions that don't lie.** A function that receives a `FrozenFrame` cannot
modify it. The caller doesn't need to defensively `.copy()` before passing data
in — that invariant is enforced structurally.

**Idempotent pipelines.** Running the same transformation on the same frame
always produces the same result. There is no shared mutable state to worry about.

**Safe caching.** Because frames are hashable, `functools.lru_cache` and other
memoization patterns work correctly:

```python
import functools

@functools.lru_cache(maxsize=128)
def compute_percentiles(df: UserMetrics) -> dict[str, float]:
    ...
```

**Set membership and deduplication.**

```python
unique_frames = {df_a, df_b, df_c}  # set of FrozenFrames
```

---

## Constructing modified frames

Because a `FrozenFrame` can't be mutated, all "modifications" produce a new
frame. This is intentional — it matches the semantics of Python's `frozenset`
and immutable value types:

```python
# filter returns a new frame — the original is untouched
top = df.filter(df.score > 8.0)

# select returns a new frame with a different schema
slim = df.select("user_id", "score")
```
