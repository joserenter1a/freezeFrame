"""
freezeFrame ecosystem interoperability.

arrow   — from_arrow / to_arrow (pyarrow RecordBatch / Table)
pandas  — from_pandas / to_pandas (Pandas 3.x, Arrow-backed)
polars  — from_polars / to_polars (zero-copy via __arrow_c_stream__)
"""

from __future__ import annotations

__all__: list[str] = []
