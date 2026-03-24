# freezeFrame — Motivations

## The Problem with Mutable DataFrames

Pandas is the dominant DataFrame library in the Python ecosystem, and for good reason — it is expressive, widely understood, and has an enormous ecosystem around it. But it was designed for interactive, exploratory data work. When you bring it into production systems, a fundamental design choice becomes a liability: **DataFrames are mutable by default.**

Any function that receives a Pandas DataFrame can silently modify it. There is no language-level or runtime-level mechanism to prevent this. The caller has no way to declare "this function will not change your data," and the callee has no way to guarantee it. The result is a class of bugs that is notoriously difficult to track down: data that looks correct at one point in a pipeline and is wrong at another, with no obvious mutation site.

This is not a hypothetical concern. It is a recurring source of incidents in production data systems — especially in pipelines with many contributors, long execution chains, or shared intermediate results.

---

## The Three Core Failures

### 1. Mutability Without Consent

A Pandas DataFrame passed to a function is fully writable. `df["new_col"] = ...`, `df.drop(inplace=True)`, `df.iloc[0] = ...` — all of these work silently on a frame you thought was safe. You can defensively call `.copy()` everywhere, but this is a convention, not a guarantee, and it has a real performance cost.

```python
def compute_score(df: pd.DataFrame) -> pd.DataFrame:
    df["score"] = df["value"] / df["total"]  # silently mutates the caller's frame
    return df
```

The caller never asked for `score` to be added to their frame. The function did it anyway. In a simple script this is annoying; in a multi-stage pipeline running in production, it is a defect waiting to happen.

### 2. Schema Opacity

A Pandas DataFrame carries no static type information about its columns. From a type checker's perspective, `df["score"]` returns `Any`. There is no way to express — and have statically enforced — "this function requires a DataFrame with columns `user_id: int`, `score: float`, and `active: bool`."

This means:
- IDEs cannot autocomplete column names
- Type checkers cannot catch column name typos or type mismatches
- Function signatures lie: `def process(df: pd.DataFrame)` tells you nothing about what shape the data needs to be
- Schema mismatches are discovered at runtime, often deep in a pipeline, far from where the bad data entered

### 3. No Hashability or Identity

Because Pandas DataFrames are mutable, they cannot be hashed. They cannot be used as dictionary keys, stored in sets, or used in any context that requires stable identity. This closes off entire categories of functional patterns — memoization, deduplication, referential equality checks — that are natural in immutable data systems.

---

## Why Now?

These problems are not new. `static-frame` attempted to address mutability over a decade ago, and it succeeded on its own terms. But the Python data ecosystem has changed substantially since then, and this motivated me to begin this project.

**Apache Arrow has become the lingua franca of data.** Pandas 3.0 added Arrow as a first-class backend. Polars is Arrow-native. DuckDB speaks Arrow. The `__arrow_c_stream__` protocol enables zero-copy data sharing between any of these systems. A modern immutable DataFrame library should be built on this foundation — not on NumPy arrays with a Python-level immutability wrapper on top.

**Python's type system has matured.** `dataclass_transform` (PEP 681) gives libraries the ability to teach type checkers about class-level field definitions — the same mechanism Pydantic uses to make model fields statically typed. TypeVarTuple (PEP 646) opens the door to expressing schema-altering operations with precise return types. PEP 695 brings cleaner generic syntax. These tools did not exist when the older generation of DataFrame libraries was designed.

**Functional programming patterns are increasingly common in Python data work.** Teams writing data pipelines with strong correctness requirements — ML feature stores, financial data systems, compliance pipelines — are reaching for immutable, composable primitives. Adding to why I wanted this tool.

---

## What freezeFrame Is

`freezeFrame` is a DataFrame library built for the cases where correctness matters more than convenience:

- **Immutable by construction.** Not by convention, not by copying — Arrow buffers are read-only at the memory level. There is no mutation to defend against because mutation is not physically possible.
- **Typed at the schema level.** Columns are declared as part of a class definition using `Column[T]`. Type checkers understand the schema. IDEs can autocomplete column names. Function signatures can express data shape requirements that are both statically checkable and runtime-validated.
- **Hashable.** A `FrozenFrame` has a stable hash based on its schema and contents. It can be used as a dictionary key, stored in a set, cached with `functools.lru_cache`. The full range of functional patterns is available.
- **Arrow-native.** Built on `pyarrow.RecordBatch` from the ground up. Zero-copy interop with Polars, DuckDB, and Pandas 3.x via the Arrow C Stream protocol.
- **Honest about nulls.** `Column[int]` means non-nullable. `Column[int | None]` means nullable. This is explicit in the schema and enforced at construction — not silently coerced.

---

## What freezeFrame Is Not

`freezeFrame` is not a replacement for Pandas in exploratory, interactive contexts. Jupyter notebooks, ad-hoc analysis, and rapid iteration are exactly what Pandas is good at, and there is no reason to change that.

`freezeFrame` is for the code that runs in production — the pipelines, the feature engineering jobs, the data validation layers, the ETL steps where a silent mutation or a schema mismatch costs real money or produces wrong results that someone has to find and fix later.

---

## The Value Proposition

> A DataFrame that cannot be mutated cannot cause mutation bugs.
> A DataFrame with a declared schema cannot silently receive the wrong columns.
> A DataFrame that is hashable can participate in any data structure or caching strategy.

These are not performance optimizations or convenience features. They are correctness guarantees — the kind that let a team ship a data pipeline and trust that it will behave the same way in production as it did in testing, and the same way next month as it does today.

That is what `freezeFrame` is for.
