# Ecosystem Interop

`freezeFrame` is built on Apache Arrow, which is the interchange format for
the modern Python data ecosystem. This means zero-copy interop with Polars,
DuckDB, and Pandas 3.x is a first-class goal.

!!! note "Phase 3"
    Full interop converters are planned for Phase 3 (`0.3.x`). This page
    describes the intended API.

---

## Apache Arrow

Every `FrozenFrame` can be converted to and from a `pyarrow.RecordBatch` or
`pyarrow.Table` with no data copies — Arrow is the internal representation.

```python
import pyarrow as pa

# from Arrow
batch: pa.RecordBatch = ...
df = UserMetrics.from_arrow(batch)

# to Arrow
batch: pa.RecordBatch = df.to_arrow()
table: pa.Table = df.to_arrow_table()
```

Requires: `pyarrow` (always installed as the core dependency).

---

## Polars

Conversion to and from Polars uses the Arrow C Stream protocol
(`__arrow_c_stream__`). No data is copied.

```python
import polars as pl

# from Polars
pl_df: pl.DataFrame = ...
df = UserMetrics.from_polars(pl_df)

# to Polars
pl_df: pl.DataFrame = df.to_polars()
```

Requires: `pip install "freezeframe[polars]"`

---

## Pandas 3.x

Pandas 3.0 introduced Arrow-backed Series and DataFrames. When the Pandas
frame uses Arrow backing, the conversion is zero-copy. NumPy-backed frames
are converted via Arrow with one copy.

```python
import pandas as pd

# from Pandas
pd_df: pd.DataFrame = ...
df = UserMetrics.from_pandas(pd_df)

# to Pandas (Arrow-backed)
pd_df: pd.DataFrame = df.to_pandas()
```

Requires: `pip install "freezeframe[pandas]"`

---

## DuckDB

```python
import duckdb

# to DuckDB relation (zero-copy via Arrow)
rel = df.to_duckdb()
result = rel.filter("score > 8.0").fetchdf()

# from DuckDB relation
df = UserMetrics.from_duckdb(rel)
```

Requires: `pip install "freezeframe[duckdb]"`

---

## Arrow C Stream protocol

`FrozenFrame` implements `__arrow_c_stream__`, which means any library that
speaks the Arrow PyCapsule Interface can consume a `FrozenFrame` directly —
no explicit converter needed:

```python
# any Arrow-compatible library
import some_arrow_lib
result = some_arrow_lib.process(df)  # df acts as an Arrow stream
```
