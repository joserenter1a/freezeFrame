"""Tests for series.py — FrozenSeries[T]."""

from __future__ import annotations

import pyarrow as pa
import pytest

from freezeframe.exceptions import FrozenFrameError
from freezeframe.series import FrozenSeries

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def int_series(*values: int | None) -> FrozenSeries:
    return FrozenSeries(pa.array(list(values), type=pa.int64()))


def float_series(*values: float | None) -> FrozenSeries:
    return FrozenSeries(pa.array(list(values), type=pa.float64()))


def str_series(*values: str | None) -> FrozenSeries:
    return FrozenSeries(pa.array(list(values), type=pa.large_utf8()))


# ---------------------------------------------------------------------------
# Construction and properties
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_wraps_array(self) -> None:
        arr = pa.array([1, 2, 3], type=pa.int64())
        s = FrozenSeries(arr)
        assert len(s) == 3

    def test_type_property(self) -> None:
        s = int_series(1, 2, 3)
        assert s.type == pa.int64()

    def test_null_count_none(self) -> None:
        s = int_series(1, 2, 3)
        assert s.null_count == 0

    def test_null_count_with_nulls(self) -> None:
        s = int_series(1, None, 3)
        assert s.null_count == 1


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


class TestImmutability:
    def test_setattr_raises(self) -> None:
        s = int_series(1, 2, 3)
        with pytest.raises(FrozenFrameError, match="immutable"):
            s._array = pa.array([4, 5, 6])  # type: ignore[misc]

    def test_delattr_raises(self) -> None:
        s = int_series(1, 2, 3)
        with pytest.raises(FrozenFrameError, match="immutable"):
            del s._array  # type: ignore[misc]

    def test_not_hashable(self) -> None:
        s = int_series(1, 2, 3)
        with pytest.raises(TypeError):
            hash(s)


# ---------------------------------------------------------------------------
# Sequence protocol
# ---------------------------------------------------------------------------


class TestSequence:
    def test_len(self) -> None:
        assert len(int_series(1, 2, 3)) == 3

    def test_len_empty(self) -> None:
        assert len(FrozenSeries(pa.array([], type=pa.int64()))) == 0

    def test_iter_yields_python_values(self) -> None:
        assert list(int_series(1, 2, 3)) == [1, 2, 3]

    def test_iter_nulls_become_none(self) -> None:
        assert list(int_series(1, None, 3)) == [1, None, 3]

    def test_getitem_int(self) -> None:
        s = int_series(10, 20, 30)
        assert s[0] == 10
        assert s[2] == 30

    def test_getitem_negative_index(self) -> None:
        s = int_series(10, 20, 30)
        assert s[-1] == 30

    def test_getitem_null_returns_none(self) -> None:
        s = int_series(1, None, 3)
        assert s[1] is None

    def test_getitem_slice_returns_series(self) -> None:
        s = int_series(1, 2, 3, 4, 5)
        sliced = s[1:3]
        assert isinstance(sliced, FrozenSeries)
        assert sliced.to_pylist() == [2, 3]

    def test_getitem_slice_preserves_type(self) -> None:
        s = int_series(1, 2, 3)
        assert s[:2].type == pa.int64()


# ---------------------------------------------------------------------------
# Comparison operators — return pa.BooleanArray
# ---------------------------------------------------------------------------


class TestComparisons:
    def test_gt_scalar(self) -> None:
        s = int_series(1, 5, 3)
        result = s > 2
        assert isinstance(result, pa.Array)
        assert result.to_pylist() == [False, True, True]

    def test_lt_scalar(self) -> None:
        result = int_series(1, 5, 3) < 4
        assert result.to_pylist() == [True, False, True]

    def test_ge_scalar(self) -> None:
        result = int_series(1, 2, 3) >= 2
        assert result.to_pylist() == [False, True, True]

    def test_le_scalar(self) -> None:
        result = int_series(1, 2, 3) <= 2
        assert result.to_pylist() == [True, True, False]

    def test_eq_scalar(self) -> None:
        result = int_series(1, 2, 3) == 2
        assert result.to_pylist() == [False, True, False]

    def test_ne_scalar(self) -> None:
        result = int_series(1, 2, 3) != 2
        assert result.to_pylist() == [True, False, True]

    def test_gt_series(self) -> None:
        a = int_series(1, 5, 3)
        b = int_series(2, 4, 3)
        result = a > b
        assert result.to_pylist() == [False, True, False]

    def test_eq_string(self) -> None:
        s = str_series("a", "b", "c")
        result = s == "b"
        assert result.to_pylist() == [False, True, False]

    def test_float_comparison(self) -> None:
        s = float_series(1.0, 8.5, 9.1)
        result = s > 8.0
        assert result.to_pylist() == [False, True, True]


# ---------------------------------------------------------------------------
# equals() — value equality
# ---------------------------------------------------------------------------


class TestEquals:
    def test_equal_series(self) -> None:
        a = int_series(1, 2, 3)
        b = int_series(1, 2, 3)
        assert a.equals(b) is True

    def test_unequal_series(self) -> None:
        a = int_series(1, 2, 3)
        b = int_series(1, 2, 4)
        assert a.equals(b) is False

    def test_null_equality(self) -> None:
        # Arrow equals() treats two nulls at same position as equal
        a = int_series(1, None, 3)
        b = int_series(1, None, 3)
        assert a.equals(b) is True

    def test_non_series_returns_not_implemented(self) -> None:
        s = int_series(1, 2, 3)
        assert s.equals("not a series") is NotImplemented  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------


class TestConversion:
    def test_to_pylist(self) -> None:
        assert int_series(1, 2, 3).to_pylist() == [1, 2, 3]

    def test_to_pylist_with_nulls(self) -> None:
        assert int_series(1, None, 3).to_pylist() == [1, None, 3]

    def test_to_arrow_returns_array(self) -> None:
        arr = pa.array([1, 2, 3], type=pa.int64())
        s = FrozenSeries(arr)
        assert s.to_arrow() is arr


# ---------------------------------------------------------------------------
# repr
# ---------------------------------------------------------------------------


class TestRepr:
    def test_repr_contains_type(self) -> None:
        s = int_series(1, 2, 3)
        assert "int64" in repr(s)

    def test_repr_contains_values(self) -> None:
        s = int_series(1, 2, 3)
        assert "[1, 2, 3]" in repr(s)

    def test_repr_prefix(self) -> None:
        s = int_series(1, 2)
        assert repr(s).startswith("FrozenSeries")
