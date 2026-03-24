"""
FrozenSeries[T] — a typed, immutable view over a single Arrow column.

Returned by FrozenFrame column access:

    df.user_id        # -> FrozenSeries[int]
    df["user_id"]     # -> FrozenSeries[int]

Supports typed iteration, __len__, __getitem__, and comparison operators
for use in filter expressions.
"""

from __future__ import annotations

__all__: list[str] = ["FrozenSeries"]


class FrozenSeries[T]:
    """Typed immutable series backed by a pyarrow ChunkedArray.

    Not yet implemented — Phase 1.
    """
