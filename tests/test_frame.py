"""Tests for frame.py — FrozenFrameMeta and FrozenFrame."""

from __future__ import annotations

import pyarrow as pa
import pytest

from freezeframe.column import field
from freezeframe.exceptions import FrozenFrameError, SchemaValidationError
from freezeframe.frame import FrozenFrame
from freezeframe.series import FrozenSeries

# ---------------------------------------------------------------------------
# Fixtures — reusable subclasses
# ---------------------------------------------------------------------------


class UserMetrics(FrozenFrame):
    user_id: int
    name: str
    score: float
    active: bool | None  # nullable


class Empty(FrozenFrame):
    pass


SAMPLE_DATA = {
    "user_id": [1, 2, 3],
    "name": ["alice", "bob", "carol"],
    "score": [9.1, 7.4, 8.8],
    "active": [True, False, None],
}


# ---------------------------------------------------------------------------
# FrozenFrameMeta — schema compilation
# ---------------------------------------------------------------------------


class TestMeta:
    def test_schema_compiled_on_subclass(self) -> None:
        assert isinstance(UserMetrics.__schema__, pa.Schema)

    def test_schema_has_correct_columns(self) -> None:
        names = UserMetrics.__schema__.names
        assert names == ["user_id", "name", "score", "active"]

    def test_non_nullable_columns(self) -> None:
        schema = UserMetrics.__schema__
        assert schema.field("user_id").nullable is False
        assert schema.field("name").nullable is False
        assert schema.field("score").nullable is False

    def test_nullable_column(self) -> None:
        assert UserMetrics.__schema__.field("active").nullable is True

    def test_arrow_types(self) -> None:
        schema = UserMetrics.__schema__
        assert schema.field("user_id").type == pa.int64()
        assert schema.field("name").type == pa.large_utf8()
        assert schema.field("score").type == pa.float64()
        assert schema.field("active").type == pa.bool_()

    def test_base_class_has_empty_schema(self) -> None:
        assert FrozenFrame.__schema__.names == []

    def test_empty_subclass_has_empty_schema(self) -> None:
        assert Empty.__schema__.names == []

    def test_field_override_applied(self) -> None:
        class Compact(FrozenFrame):
            score: float = field(arrow_type=pa.float32())

        assert Compact.__schema__.field("score").type == pa.float32()

    def test_multiple_subclasses_have_independent_schemas(self) -> None:
        class A(FrozenFrame):
            x: int

        class B(FrozenFrame):
            y: str

        assert A.__schema__.names == ["x"]
        assert B.__schema__.names == ["y"]


# ---------------------------------------------------------------------------
# from_dict — construction
# ---------------------------------------------------------------------------


class TestFromDict:
    def test_basic_construction(self) -> None:
        df = UserMetrics.from_dict(SAMPLE_DATA)
        assert len(df) == 3

    def test_extra_key_raises(self) -> None:
        bad = {**SAMPLE_DATA, "extra": [1, 2, 3]}
        with pytest.raises(SchemaValidationError, match="Unexpected key"):
            UserMetrics.from_dict(bad)

    def test_missing_key_raises(self) -> None:
        bad = {k: v for k, v in SAMPLE_DATA.items() if k != "score"}
        with pytest.raises(SchemaValidationError, match="Missing key"):
            UserMetrics.from_dict(bad)

    def test_null_in_non_nullable_raises(self) -> None:
        bad = {**SAMPLE_DATA, "score": [1.0, None, 3.0]}
        with pytest.raises(SchemaValidationError):
            UserMetrics.from_dict(bad)

    def test_validate_false_skips_null_check(self) -> None:
        # Build a batch where non-nullable has nulls — bypassed with validate=False
        data = {**SAMPLE_DATA, "score": [1.0, None, 3.0]}
        # from_dict casts via pa.array — None in a float column becomes null
        # validate=False should not raise
        df = UserMetrics.from_dict(data, validate=False)
        assert len(df) == 3

    def test_returns_correct_subtype(self) -> None:
        df = UserMetrics.from_dict(SAMPLE_DATA)
        assert isinstance(df, UserMetrics)
        assert isinstance(df, FrozenFrame)

    def test_type_error_on_bad_data(self) -> None:
        bad = {**SAMPLE_DATA, "user_id": ["not", "an", "int"]}
        with pytest.raises(SchemaValidationError, match="could not convert"):
            UserMetrics.from_dict(bad)


# ---------------------------------------------------------------------------
# from_arrow — construction
# ---------------------------------------------------------------------------


class TestFromArrow:
    def _make_batch(self) -> pa.RecordBatch:
        schema = UserMetrics.__schema__
        return pa.record_batch(
            [
                pa.array([1, 2], type=pa.int64()),
                pa.array(["a", "b"], type=pa.large_utf8()),
                pa.array([1.0, 2.0], type=pa.float64()),
                pa.array([True, None], type=pa.bool_()),
            ],
            schema=schema,
        )

    def test_basic_construction(self) -> None:
        df = UserMetrics.from_arrow(self._make_batch())
        assert len(df) == 2

    def test_schema_mismatch_raises(self) -> None:
        wrong = pa.record_batch(
            [pa.array([1, 2], type=pa.int32())],
            schema=pa.schema([pa.field("user_id", pa.int32())]),
        )
        with pytest.raises(SchemaValidationError):
            UserMetrics.from_arrow(wrong)

    def test_validate_false_skips_check(self) -> None:
        wrong = pa.record_batch(
            [pa.array([1, 2], type=pa.int32())],
            schema=pa.schema([pa.field("user_id", pa.int32())]),
        )
        # Should not raise
        df = UserMetrics.from_arrow(wrong, validate=False)
        assert len(df) == 2


# ---------------------------------------------------------------------------
# Immutability guards
# ---------------------------------------------------------------------------


class TestImmutability:
    def setup_method(self) -> None:
        self.df = UserMetrics.from_dict(SAMPLE_DATA)

    def test_setitem_raises(self) -> None:
        with pytest.raises(FrozenFrameError, match="immutable"):
            self.df["score"] = [1.0, 2.0, 3.0]

    def test_setattr_raises(self) -> None:
        with pytest.raises(FrozenFrameError, match="immutable"):
            self.df.score = [1.0, 2.0, 3.0]

    def test_delitem_raises(self) -> None:
        with pytest.raises(FrozenFrameError, match="immutable"):
            del self.df["score"]

    def test_delattr_raises(self) -> None:
        with pytest.raises(FrozenFrameError, match="immutable"):
            del self.df.score

    def test_error_names_attribute(self) -> None:
        with pytest.raises(FrozenFrameError, match="score"):
            self.df["score"] = []


# ---------------------------------------------------------------------------
# Column access
# ---------------------------------------------------------------------------


class TestColumnAccess:
    def setup_method(self) -> None:
        self.df = UserMetrics.from_dict(SAMPLE_DATA)

    def test_getitem_returns_frozen_series(self) -> None:
        col = self.df["score"]
        assert isinstance(col, FrozenSeries)
        assert col.to_pylist() == [9.1, 7.4, 8.8]

    def test_getattr_returns_frozen_series(self) -> None:
        col = self.df.score
        assert isinstance(col, FrozenSeries)
        assert col.to_pylist() == [9.1, 7.4, 8.8]

    def test_getitem_missing_column_raises(self) -> None:
        with pytest.raises(KeyError, match="nonexistent"):
            self.df["nonexistent"]

    def test_getattr_missing_column_raises(self) -> None:
        with pytest.raises(AttributeError):
            _ = self.df.nonexistent  # type: ignore[attr-defined]

    def test_nullable_column_values(self) -> None:
        col = self.df["active"]
        assert isinstance(col, FrozenSeries)
        assert col.to_pylist() == [True, False, None]


# ---------------------------------------------------------------------------
# Sequence protocol
# ---------------------------------------------------------------------------


class TestSequence:
    def setup_method(self) -> None:
        self.df = UserMetrics.from_dict(SAMPLE_DATA)

    def test_len(self) -> None:
        assert len(self.df) == 3

    def test_iter_yields_dicts(self) -> None:
        rows = list(self.df)
        assert len(rows) == 3
        assert rows[0] == {
            "user_id": 1,
            "name": "alice",
            "score": 9.1,
            "active": True,
        }

    def test_iter_preserves_nulls(self) -> None:
        rows = list(self.df)
        assert rows[2]["active"] is None


# ---------------------------------------------------------------------------
# Equality and hashing
# ---------------------------------------------------------------------------


class TestEqualityAndHash:
    def setup_method(self) -> None:
        self.df = UserMetrics.from_dict(SAMPLE_DATA)

    def test_equal_to_itself(self) -> None:
        assert self.df == self.df

    def test_equal_to_identical_frame(self) -> None:
        other = UserMetrics.from_dict(SAMPLE_DATA)
        assert self.df == other

    def test_not_equal_to_different_data(self) -> None:
        different = {**SAMPLE_DATA, "score": [1.0, 2.0, 3.0]}
        other = UserMetrics.from_dict(different)
        assert self.df != other

    def test_not_equal_to_different_schema(self) -> None:
        class Other(FrozenFrame):
            x: int

        other = Other.from_dict({"x": [1, 2, 3]})
        assert self.df != other

    def test_not_equal_to_non_frame(self) -> None:
        assert self.df.__eq__("not a frame") is NotImplemented

    def test_hashable(self) -> None:
        h = hash(self.df)
        assert isinstance(h, int)

    def test_hash_stable(self) -> None:
        assert hash(self.df) == hash(self.df)

    def test_equal_frames_have_equal_hashes(self) -> None:
        other = UserMetrics.from_dict(SAMPLE_DATA)
        assert hash(self.df) == hash(other)

    def test_usable_as_dict_key(self) -> None:
        cache = {self.df: "result"}
        assert cache[self.df] == "result"

    def test_usable_in_set(self) -> None:
        s = {self.df, self.df}
        assert len(s) == 1


# ---------------------------------------------------------------------------
# repr
# ---------------------------------------------------------------------------


class TestRepr:
    def test_repr_contains_class_name(self) -> None:
        df = UserMetrics.from_dict(SAMPLE_DATA)
        assert "UserMetrics" in repr(df)

    def test_repr_contains_row_count(self) -> None:
        df = UserMetrics.from_dict(SAMPLE_DATA)
        assert "3" in repr(df)

    def test_repr_contains_column_names(self) -> None:
        df = UserMetrics.from_dict(SAMPLE_DATA)
        r = repr(df)
        assert "score" in r
        assert "user_id" in r
