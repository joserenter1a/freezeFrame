"""Tests for column.py — field(), register_type(), resolve_arrow_type()."""

from __future__ import annotations

import datetime
import decimal

import pyarrow as pa
import pytest

from freezeframe.column import (
    _PYTHON_TO_ARROW,
    field,
    register_type,
    resolve_arrow_type,
)

# ---------------------------------------------------------------------------
# resolve_arrow_type — bare types
# ---------------------------------------------------------------------------


class TestResolveBaretypes:
    def test_int(self) -> None:
        arrow_type, nullable = resolve_arrow_type(int)
        assert arrow_type == pa.int64()
        assert nullable is False

    def test_float(self) -> None:
        arrow_type, nullable = resolve_arrow_type(float)
        assert arrow_type == pa.float64()
        assert nullable is False

    def test_str(self) -> None:
        arrow_type, nullable = resolve_arrow_type(str)
        assert arrow_type == pa.large_utf8()
        assert nullable is False

    def test_bool(self) -> None:
        arrow_type, nullable = resolve_arrow_type(bool)
        assert arrow_type == pa.bool_()
        assert nullable is False

    def test_bytes(self) -> None:
        arrow_type, nullable = resolve_arrow_type(bytes)
        assert arrow_type == pa.large_binary()
        assert nullable is False

    def test_datetime(self) -> None:
        arrow_type, nullable = resolve_arrow_type(datetime.datetime)
        assert arrow_type == pa.timestamp("us", tz="UTC")
        assert nullable is False

    def test_date(self) -> None:
        arrow_type, nullable = resolve_arrow_type(datetime.date)
        assert arrow_type == pa.date32()
        assert nullable is False

    def test_timedelta(self) -> None:
        arrow_type, nullable = resolve_arrow_type(datetime.timedelta)
        assert arrow_type == pa.duration("us")
        assert nullable is False

    def test_decimal(self) -> None:
        arrow_type, nullable = resolve_arrow_type(decimal.Decimal)
        assert arrow_type == pa.decimal128(38, 18)
        assert nullable is False


# ---------------------------------------------------------------------------
# resolve_arrow_type — nullable unions
# ---------------------------------------------------------------------------


class TestResolveNullable:
    def test_int_or_none(self) -> None:
        arrow_type, nullable = resolve_arrow_type(int | None)
        assert arrow_type == pa.int64()
        assert nullable is True

    def test_str_or_none(self) -> None:
        arrow_type, nullable = resolve_arrow_type(str | None)
        assert arrow_type == pa.large_utf8()
        assert nullable is True

    def test_optional_float(self) -> None:
        # typing.Optional[T] form — still supported for compatibility
        import typing

        arrow_type, nullable = resolve_arrow_type(typing.Optional[float])  # noqa: UP045
        assert arrow_type == pa.float64()
        assert nullable is True

    def test_bool_or_none(self) -> None:
        arrow_type, nullable = resolve_arrow_type(bool | None)
        assert arrow_type == pa.bool_()
        assert nullable is True


# ---------------------------------------------------------------------------
# resolve_arrow_type — arrow_type override
# ---------------------------------------------------------------------------


class TestResolveOverride:
    def test_float32_override(self) -> None:
        arrow_type, nullable = resolve_arrow_type(float, override=pa.float32())
        assert arrow_type == pa.float32()
        assert nullable is False

    def test_nullable_with_override(self) -> None:
        arrow_type, nullable = resolve_arrow_type(float | None, override=pa.float32())
        assert arrow_type == pa.float32()
        assert nullable is True

    def test_override_ignores_type_map(self) -> None:
        # Even an unmapped type works if an override is provided
        class CustomType:
            pass

        arrow_type, _nullable = resolve_arrow_type(CustomType, override=pa.int8())
        assert arrow_type == pa.int8()


# ---------------------------------------------------------------------------
# resolve_arrow_type — error cases
# ---------------------------------------------------------------------------


class TestResolveErrors:
    def test_unknown_type_raises(self) -> None:
        class Unknown:
            pass

        with pytest.raises(TypeError, match="No Arrow type mapping"):
            resolve_arrow_type(Unknown)

    def test_multi_member_union_raises(self) -> None:
        with pytest.raises(TypeError, match="Unsupported union annotation"):
            resolve_arrow_type(int | str)

    def test_three_way_union_raises(self) -> None:
        with pytest.raises(TypeError, match="Unsupported union annotation"):
            resolve_arrow_type(int | str | None)


# ---------------------------------------------------------------------------
# field()
# ---------------------------------------------------------------------------


class TestField:
    def test_empty_field(self) -> None:
        f = field()
        assert f.arrow_type is None
        assert f.description == ""

    def test_arrow_type_stored(self) -> None:
        f = field(arrow_type=pa.float32())
        assert f.arrow_type == pa.float32()

    def test_description_stored(self) -> None:
        f = field(description="the user score")
        assert f.description == "the user score"

    def test_repr_empty(self) -> None:
        assert repr(field()) == "field()"

    def test_repr_with_arrow_type(self) -> None:
        r = repr(field(arrow_type=pa.float32()))
        assert "arrow_type" in r
        assert repr(pa.float32()) in r

    def test_repr_with_description(self) -> None:
        r = repr(field(description="hello"))
        assert "hello" in r


# ---------------------------------------------------------------------------
# register_type()
# ---------------------------------------------------------------------------


class TestRegisterType:
    def test_registered_type_resolves(self) -> None:
        class MyType:
            pass

        register_type(MyType, pa.int8())
        arrow_type, nullable = resolve_arrow_type(MyType)
        assert arrow_type == pa.int8()
        assert nullable is False

    def test_registered_type_nullable(self) -> None:
        class AnotherType:
            pass

        register_type(AnotherType, pa.int16())
        arrow_type, nullable = resolve_arrow_type(AnotherType | None)
        assert arrow_type == pa.int16()
        assert nullable is True

    def test_overwrite_existing_mapping(self) -> None:
        # Overwriting a built-in mapping is allowed (intentional escape hatch)
        original = _PYTHON_TO_ARROW[int]
        try:
            register_type(int, pa.int32())
            arrow_type, _ = resolve_arrow_type(int)
            assert arrow_type == pa.int32()
        finally:
            # Restore the original mapping so other tests are unaffected
            register_type(int, original)
