"""Tests for schema.py — build_schema() and validate()."""

from __future__ import annotations

import datetime
import decimal

import pyarrow as pa
import pytest

from freezeframe.column import field
from freezeframe.exceptions import SchemaValidationError
from freezeframe.schema import build_schema, validate

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_batch(schema: pa.Schema, data: dict) -> pa.RecordBatch:
    """Build a RecordBatch from a dict using the given schema's column order."""
    arrays = [pa.array(data[f.name], type=f.type) for f in schema]
    return pa.record_batch(arrays, schema=schema)


# ---------------------------------------------------------------------------
# build_schema
# ---------------------------------------------------------------------------


class TestBuildSchema:
    def test_basic_types(self) -> None:
        annotations = {"user_id": int, "name": str, "score": float, "active": bool}
        schema = build_schema(annotations, {})

        assert schema.field("user_id").type == pa.int64()
        assert schema.field("name").type == pa.large_utf8()
        assert schema.field("score").type == pa.float64()
        assert schema.field("active").type == pa.bool_()

    def test_all_fields_non_nullable_by_default(self) -> None:
        annotations = {"user_id": int, "score": float}
        schema = build_schema(annotations, {})

        assert schema.field("user_id").nullable is False
        assert schema.field("score").nullable is False

    def test_nullable_via_union(self) -> None:
        annotations = {"score": float | None, "label": str | None}
        schema = build_schema(annotations, {})

        assert schema.field("score").nullable is True
        assert schema.field("label").nullable is True

    def test_non_nullable_mixed_with_nullable(self) -> None:
        annotations = {"user_id": int, "score": float | None}
        schema = build_schema(annotations, {})

        assert schema.field("user_id").nullable is False
        assert schema.field("score").nullable is True

    def test_field_arrow_type_override(self) -> None:
        annotations = {"score": float}
        defaults = {"score": field(arrow_type=pa.float32())}
        schema = build_schema(annotations, defaults)

        assert schema.field("score").type == pa.float32()

    def test_field_override_preserves_nullability(self) -> None:
        annotations = {"score": float | None}
        defaults = {"score": field(arrow_type=pa.float32())}
        schema = build_schema(annotations, defaults)

        assert schema.field("score").type == pa.float32()
        assert schema.field("score").nullable is True

    def test_field_description_has_no_schema_effect(self) -> None:
        annotations = {"label": str}
        defaults = {"label": field(description="some label")}
        schema = build_schema(annotations, defaults)

        assert schema.field("label").type == pa.large_utf8()
        assert schema.field("label").nullable is False

    def test_non_field_defaults_are_ignored(self) -> None:
        # class-level defaults that are not field() instances should be skipped
        annotations = {"score": float}
        defaults = {"score": 0.0}  # plain default value, not a field()
        schema = build_schema(annotations, defaults)

        assert schema.field("score").type == pa.float64()

    def test_column_order_preserved(self) -> None:
        annotations = {"c": str, "a": int, "b": float}
        schema = build_schema(annotations, {})

        assert schema.names == ["c", "a", "b"]

    def test_datetime_type(self) -> None:
        annotations = {"ts": datetime.datetime}
        schema = build_schema(annotations, {})

        assert schema.field("ts").type == pa.timestamp("us", tz="UTC")

    def test_decimal_type(self) -> None:
        annotations = {"amount": decimal.Decimal}
        schema = build_schema(annotations, {})

        assert schema.field("amount").type == pa.decimal128(38, 18)

    def test_empty_annotations(self) -> None:
        schema = build_schema({}, {})
        assert schema.names == []


# ---------------------------------------------------------------------------
# validate — passing cases
# ---------------------------------------------------------------------------


class TestValidatePassing:
    def test_exact_match(self) -> None:
        schema = build_schema({"user_id": int, "score": float}, {})
        batch = make_batch(schema, {"user_id": [1, 2], "score": [9.1, 7.4]})
        validate(schema, batch)  # must not raise

    def test_nullable_column_with_nulls(self) -> None:
        schema = build_schema({"score": float | None}, {})
        batch = make_batch(schema, {"score": [1.0, None, 3.0]})
        validate(schema, batch)  # must not raise

    def test_nullable_column_without_nulls(self) -> None:
        # nullable=True but no actual nulls — still valid
        schema = build_schema({"score": float | None}, {})
        batch = make_batch(schema, {"score": [1.0, 2.0, 3.0]})
        validate(schema, batch)  # must not raise

    def test_single_column(self) -> None:
        schema = build_schema({"name": str}, {})
        batch = make_batch(schema, {"name": ["alice", "bob"]})
        validate(schema, batch)  # must not raise


# ---------------------------------------------------------------------------
# validate — SchemaValidationError cases
# ---------------------------------------------------------------------------


class TestValidateErrors:
    def test_extra_column_raises(self) -> None:
        schema = build_schema({"user_id": int}, {})
        # Manually build a batch with an extra column
        extra_schema = pa.schema(
            [
                pa.field("user_id", pa.int64(), nullable=False),
                pa.field("extra", pa.int64(), nullable=False),
            ]
        )
        batch = pa.record_batch(
            [pa.array([1, 2]), pa.array([9, 8])],
            schema=extra_schema,
        )
        with pytest.raises(SchemaValidationError, match="Unexpected column"):
            validate(schema, batch)

    def test_missing_column_raises(self) -> None:
        schema = build_schema({"user_id": int, "score": float}, {})
        # Batch is missing "score"
        partial_schema = pa.schema([pa.field("user_id", pa.int64(), nullable=False)])
        batch = pa.record_batch([pa.array([1, 2])], schema=partial_schema)
        with pytest.raises(SchemaValidationError, match="Missing column"):
            validate(schema, batch)

    def test_type_mismatch_raises(self) -> None:
        schema = build_schema({"user_id": int}, {})
        # Provide int32 where int64 is expected
        wrong_schema = pa.schema([pa.field("user_id", pa.int32(), nullable=False)])
        batch = pa.record_batch(
            [pa.array([1, 2], type=pa.int32())], schema=wrong_schema
        )
        with pytest.raises(SchemaValidationError, match="expected Arrow type"):
            validate(schema, batch)

    def test_null_in_non_nullable_column_raises(self) -> None:
        schema = build_schema({"score": float}, {})
        # Force nulls into a non-nullable column via a nullable schema
        nullable_schema = pa.schema([pa.field("score", pa.float64(), nullable=True)])
        batch = pa.record_batch(
            [pa.array([1.0, None, 3.0], type=pa.float64())],
            schema=nullable_schema,
        )
        with pytest.raises(SchemaValidationError, match="non-nullable"):
            validate(schema, batch)

    def test_error_message_names_column(self) -> None:
        schema = build_schema({"score": float}, {})
        wrong_schema = pa.schema([pa.field("score", pa.int64(), nullable=False)])
        batch = pa.record_batch([pa.array([1, 2])], schema=wrong_schema)
        with pytest.raises(SchemaValidationError, match="'score'"):
            validate(schema, batch)
