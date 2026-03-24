"""
freezeFrame functional transform operations.

Schema-preserving (return the same concrete FrozenFrame subtype):
    filter, sort, slice

Schema-altering (return a new schema; full static typing deferred to Phase 3):
    select, rename, drop, with_column, join
"""

from __future__ import annotations

__all__: list[str] = []
