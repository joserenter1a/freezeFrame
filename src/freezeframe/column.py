"""
field() and the Python→Arrow type mapping.

This module is the foundation of the schema system.  Everything that converts
a FrozenFrame class definition into a pyarrow.Schema goes through here.

Public surface
--------------
field()           Optional per-column metadata (arrow_type override, description).
register_type()   Register a custom Python type → Arrow type mapping globally.

Internal surface (used by schema.py)
-------------------------------------
resolve_arrow_type()  Resolve a single annotation to (pa.DataType, nullable: bool).
"""

from __future__ import annotations

import datetime
import decimal
import types
import typing
from typing import Any

import pyarrow as pa

__all__: list[str] = ["field", "register_type"]

# ---------------------------------------------------------------------------
# Default Python → Arrow type map
# ---------------------------------------------------------------------------
# bool must come before int because bool is a subclass of int in Python;
# dict lookup order doesn't matter here since we key on the exact type, but
# it is good practice to document the ordering constraint.

_PYTHON_TO_ARROW: dict[type, pa.DataType] = {
    bool: pa.bool_(),
    int: pa.int64(),
    float: pa.float64(),
    str: pa.large_utf8(),
    bytes: pa.large_binary(),
    datetime.datetime: pa.timestamp("us", tz="UTC"),
    datetime.date: pa.date32(),
    datetime.timedelta: pa.duration("us"),
    decimal.Decimal: pa.decimal128(38, 18),
}


def register_type(python_type: type, arrow_type: pa.DataType) -> None:
    """Register a global mapping from a Python type to an Arrow DataType.

    Use this to teach freezeFrame about third-party types so they can be used
    as bare annotations in any FrozenFrame subclass.

    Parameters
    ----------
    python_type:
        The Python type to map.  Must be a concrete type (not a generic alias).
    arrow_type:
        The Arrow DataType to associate with it.

    Examples
    --------
    >>> import pyarrow as pa
    >>> import numpy as np
    >>> register_type(np.float32, pa.float32())

    After registration, the type is available as a bare annotation:

    >>> class MyFrame(FrozenFrame):
    ...     value: np.float32
    """
    _PYTHON_TO_ARROW[python_type] = arrow_type


# ---------------------------------------------------------------------------
# field()
# ---------------------------------------------------------------------------


class field:
    """Optional per-column metadata for FrozenFrame field definitions.

    ``field()`` is only needed when you want to override the default
    Python→Arrow type mapping or attach documentation to a column.  For the
    common case, a bare type annotation is sufficient.

    Parameters
    ----------
    arrow_type:
        Override the Arrow DataType for this column.  If omitted, the type is
        resolved from the annotation via the default mapping.
    description:
        Human-readable documentation for this column.  Has no runtime effect.

    Examples
    --------
    >>> import pyarrow as pa
    >>> from freezeframe import FrozenFrame, field
    >>>
    >>> class Events(FrozenFrame):
    ...     user_id: int
    ...     score:   float = field(arrow_type=pa.float32())
    ...     label:   str   = field(description="event classification label")
    """

    def __init__(
        self,
        *,
        arrow_type: pa.DataType | None = None,
        description: str = "",
    ) -> None:
        self.arrow_type = arrow_type
        self.description = description

    def __repr__(self) -> str:
        parts = []
        if self.arrow_type is not None:
            parts.append(f"arrow_type={self.arrow_type!r}")
        if self.description:
            parts.append(f"description={self.description!r}")
        return f"field({', '.join(parts)})"


# ---------------------------------------------------------------------------
# Internal resolver — used by schema.py, not part of the public API
# ---------------------------------------------------------------------------


def resolve_arrow_type(
    annotation: Any,
    override: pa.DataType | None = None,
) -> tuple[pa.DataType, bool]:
    """Resolve a Python type annotation to ``(pa.DataType, nullable)``.

    Handles bare types (``int``, ``str``, …) and ``T | None`` unions.
    ``Optional[T]`` (``typing.Union[T, None]``) is also accepted for
    compatibility, but ``T | None`` is preferred.

    Parameters
    ----------
    annotation:
        The raw value from ``__annotations__``.
    override:
        If provided (from a ``field(arrow_type=...)`` declaration), this
        Arrow type is returned directly and the type map is not consulted.
        Nullability is still derived from the annotation.

    Returns
    -------
    tuple[pa.DataType, bool]
        The resolved Arrow type and whether the column is nullable.

    Raises
    ------
    TypeError
        If the annotation is a multi-member union (other than ``T | None``),
        or if the inner type has no registered Arrow mapping.
    """
    nullable = False
    inner = annotation

    # T | None  — Python 3.10+ union syntax (types.UnionType)
    if isinstance(annotation, types.UnionType):
        args = typing.get_args(annotation)
        non_none = [a for a in args if a is not type(None)]
        if type(None) in args and len(non_none) == 1:
            nullable = True
            inner = non_none[0]
        else:
            raise TypeError(
                f"Unsupported union annotation: {annotation!r}. "
                "Only 'T | None' unions are supported — use a single type plus None."
            )

    # Optional[T] / Union[T, None]  — typing module form
    elif typing.get_origin(annotation) is typing.Union:
        args = typing.get_args(annotation)
        non_none = [a for a in args if a is not type(None)]
        if type(None) in args and len(non_none) == 1:
            nullable = True
            inner = non_none[0]
        else:
            raise TypeError(
                f"Unsupported union annotation: {annotation!r}. "
                "Only 'Optional[T]' / 'Union[T, None]' unions are supported."
            )

    # Arrow type override from field(arrow_type=...) takes precedence
    if override is not None:
        return override, nullable

    if inner not in _PYTHON_TO_ARROW:
        raise TypeError(
            f"No Arrow type mapping found for {inner!r}. "
            "Use field(arrow_type=...) to specify an explicit Arrow type, "
            "or call register_type() to add a global mapping for this type."
        )

    return _PYTHON_TO_ARROW[inner], nullable
