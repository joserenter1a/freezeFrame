"""
FrozenSeries[T] — a typed, immutable view over a single Arrow column.

Returned by FrozenFrame column access::

    df.score          # -> FrozenSeries[float]
    df["score"]       # -> FrozenSeries[float]

Comparison operators produce a ``pa.BooleanArray`` mask for use with
``FrozenFrame.filter()``::

    mask = df.score > 8.0   # pa.BooleanArray
    top  = df.filter(mask)  # FrozenFrame (Phase 2)

``FrozenSeries`` is intentionally not hashable — ``__eq__`` is element-wise,
returning a mask rather than a single bool.  Use ``series.equals(other)``
for value equality between two series.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, overload

import pyarrow as pa
import pyarrow.compute as pc

from freezeframe.exceptions import FrozenFrameError

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__: list[str] = ["FrozenSeries"]


class FrozenSeries[T]:
    """Typed, immutable series backed by a ``pyarrow.Array``.

    The type parameter ``T`` reflects the Python type of each element and is
    used by static analysis tools.  At runtime the Arrow type is available
    via the ``type`` property.

    Parameters
    ----------
    array:
        The underlying ``pa.Array``.  Must not be mutated externally after
        construction.
    """

    # Explicitly unhashable — __eq__ returns a mask, not a bool.
    __hash__ = None

    # Declared here so ty/mypy can resolve self._array; set via object.__setattr__
    # in __init__ to bypass our own mutation guard.
    _array: pa.Array

    def __init__(self, array: pa.Array) -> None:
        object.__setattr__(self, "_array", array)

    # ------------------------------------------------------------------
    # Immutability
    # ------------------------------------------------------------------

    def __setattr__(self, name: str, value: Any) -> None:
        raise FrozenFrameError(
            f"'FrozenSeries' is immutable — cannot set attribute '{name}'."
        )

    def __delattr__(self, name: str) -> None:
        raise FrozenFrameError(
            f"'FrozenSeries' is immutable — cannot delete attribute '{name}'."
        )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def type(self) -> pa.DataType:
        """The Arrow DataType of this series."""
        return self._array.type

    @property
    def null_count(self) -> int:
        """Number of null values."""
        return self._array.null_count

    # ------------------------------------------------------------------
    # Sequence protocol
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        """Number of elements."""
        return len(self._array)

    def __iter__(self) -> Iterator[T]:
        """Iterate over elements as Python values (nulls become ``None``)."""
        for scalar in self._array:
            yield scalar.as_py()

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> FrozenSeries[T]: ...

    def __getitem__(self, index: int | slice) -> T | FrozenSeries[T]:
        """Return a single element or a sliced sub-series.

        Parameters
        ----------
        index:
            An integer index returns the element as a Python value (nullable
            columns may return ``None``).  A slice returns a new
            ``FrozenSeries`` over the selected range.
        """
        if isinstance(index, slice):
            return FrozenSeries(self._array[index])
        return self._array[index].as_py()

    # ------------------------------------------------------------------
    # Comparison operators — return pa.BooleanArray for filter expressions
    # ------------------------------------------------------------------

    def _rhs(self, other: Any) -> Any:
        """Unwrap a FrozenSeries operand to its underlying pa.Array."""
        return other._array if isinstance(other, FrozenSeries) else other

    def __eq__(self, other: Any) -> pa.Array:
        """Element-wise equality — returns a boolean ``pa.Array``."""
        return pc.equal(self._array, self._rhs(other))  # type: ignore[attr-defined]

    def __ne__(self, other: Any) -> pa.Array:
        """Element-wise inequality — returns a boolean ``pa.Array``."""
        return pc.not_equal(self._array, self._rhs(other))  # type: ignore[attr-defined]

    def __lt__(self, other: Any) -> pa.Array:
        """Element-wise less-than — returns a boolean ``pa.Array``."""
        return pc.less(self._array, self._rhs(other))  # type: ignore[attr-defined]

    def __le__(self, other: Any) -> pa.Array:
        """Element-wise less-than-or-equal — returns a boolean ``pa.Array``."""
        return pc.less_equal(self._array, self._rhs(other))  # type: ignore[attr-defined]

    def __gt__(self, other: Any) -> pa.Array:
        """Element-wise greater-than — returns a boolean ``pa.Array``."""
        return pc.greater(self._array, self._rhs(other))  # type: ignore[attr-defined]

    def __ge__(self, other: Any) -> pa.Array:
        """Element-wise greater-than-or-equal — returns a boolean ``pa.Array``."""
        return pc.greater_equal(self._array, self._rhs(other))  # type: ignore[attr-defined]

    # ------------------------------------------------------------------
    # Value equality (not element-wise)
    # ------------------------------------------------------------------

    def equals(self, other: FrozenSeries[Any]) -> bool:
        """Return ``True`` if both series have identical type and values.

        Unlike ``==``, this returns a single ``bool`` and correctly handles
        null values (two nulls at the same position are considered equal).

        Parameters
        ----------
        other:
            The series to compare against.
        """
        if not isinstance(other, FrozenSeries):
            return NotImplemented
        return self._array.equals(other._array)

    # ------------------------------------------------------------------
    # Conversion helpers
    # ------------------------------------------------------------------

    def to_pylist(self) -> list[T | None]:
        """Return elements as a plain Python list (nulls become ``None``)."""
        return self._array.to_pylist()

    def to_arrow(self) -> pa.Array:
        """Return the underlying ``pa.Array`` directly."""
        return self._array

    # ------------------------------------------------------------------
    # Display
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        values = self._array.to_pylist()
        return f"FrozenSeries[{self._array.type}]({values!r})"
