"""
FrozenFrame — the core immutable DataFrame class.

Schema is declared using native Python type annotations on a subclass.
Nullable columns use ``T | None``.  ``field()`` is available for Arrow type
overrides or column documentation, but is never required::

    class UserMetrics(FrozenFrame):
        user_id: int
        name:    str
        score:   float
        active:  bool | None   # nullable

Construction::

    df = UserMetrics.from_dict({
        "user_id": [1, 2],
        "name":    ["alice", "bob"],
        "score":   [9.1, 7.4],
        "active":  [True, None],
    })
    df = UserMetrics.from_arrow(record_batch)

Immutability::

    df["score"] = ...     # raises FrozenFrameError
    df.score = ...        # raises FrozenFrameError

Hashability::

    cache = {df: result}  # works — FrozenFrame is hashable
"""

from __future__ import annotations

import hashlib
import io
import typing
from typing import Any, ClassVar, Self

import pyarrow as pa

from freezeframe.column import field
from freezeframe.exceptions import FrozenFrameError, SchemaValidationError
from freezeframe.schema import build_schema
from freezeframe.schema import validate as _validate_batch

__all__: list[str] = ["FrozenFrame"]


@typing.dataclass_transform()
class FrozenFrameMeta(type):
    """Metaclass for FrozenFrame.

    Reads ``__annotations__`` at class creation time, resolves any forward
    references, collects ``field()`` defaults, and compiles the result into a
    ``pa.Schema`` stored as ``cls.__schema__``.
    """

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> FrozenFrameMeta:
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)

        # FrozenFrame itself has no columns — skip schema compilation.
        # Any class whose bases include a FrozenFrameMeta instance is a subclass.
        if not any(isinstance(b, FrozenFrameMeta) for b in bases):
            cls.__schema__ = pa.schema([])  # type: ignore[attr-defined]
            return cls

        # Own annotations only — do not inherit parent columns.
        own_annotations: dict[str, Any] = namespace.get("__annotations__", {})

        # Resolve forward references produced by `from __future__ import annotations`.
        try:
            resolved = typing.get_type_hints(cls)
            annotations = {k: resolved[k] for k in own_annotations if k in resolved}
        except Exception:
            # Fallback: use raw annotations if resolution fails (e.g. missing imports
            # at class-definition time in a partially-constructed module).
            annotations = dict(own_annotations)

        # Collect field() defaults declared in this class body only.
        defaults = {k: v for k, v in namespace.items() if isinstance(v, field)}

        cls.__schema__ = build_schema(annotations, defaults)  # type: ignore[attr-defined]
        return cls


class FrozenFrame(metaclass=FrozenFrameMeta):
    """Immutable, schema-typed DataFrame backed by a ``pyarrow.RecordBatch``.

    Subclass and annotate with native Python types to declare a schema::

        class UserMetrics(FrozenFrame):
            user_id: int
            name:    str
            score:   float
            active:  bool | None

    See the module docstring for construction and usage examples.
    """

    __schema__: ClassVar[pa.Schema]

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(self, batch: pa.RecordBatch, *, validate: bool = True) -> None:
        """Construct directly from a ``pa.RecordBatch``.

        Prefer ``from_dict`` or ``from_arrow`` for everyday use — they
        provide clearer error messages when data doesn't match the schema.

        Parameters
        ----------
        batch:
            The Arrow RecordBatch to wrap.
        validate:
            When ``True`` (default), validate the batch against the declared
            schema before storing.  Pass ``False`` only when the data is
            already known to be valid, e.g. inside transform operations.
        """
        if validate:
            _validate_batch(type(self).__schema__, batch)
        # Use object.__setattr__ to bypass our own mutation guard.
        object.__setattr__(self, "_batch", batch)

    @classmethod
    def _from_batch(cls, batch: pa.RecordBatch) -> Self:
        """Internal fast-path constructor — skips validation.

        For use by transform operations (filter, sort, select, …) that
        already hold a structurally-valid batch.
        """
        instance = object.__new__(cls)
        object.__setattr__(instance, "_batch", batch)
        return instance

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        *,
        validate: bool = True,
    ) -> Self:
        """Construct from a column-oriented dictionary.

        Parameters
        ----------
        data:
            Mapping of column name to a sequence of values (list, numpy
            array, or any iterable accepted by ``pa.array``).
        validate:
            Run schema validation after building the batch.  Defaults to
            ``True``.  Pass ``False`` only in hot paths where the data is
            already trusted.

        Returns
        -------
        Self
            A new instance of the concrete subclass.

        Raises
        ------
        SchemaValidationError
            If the data has unexpected or missing columns, type mismatches,
            or null values in a non-nullable column.
        """
        schema = cls.__schema__

        declared = set(schema.names)
        provided = set(data.keys())

        if extra := provided - declared:
            raise SchemaValidationError(
                f"Unexpected key(s) not declared in schema: {sorted(extra)}. "
                "Remove them or add matching annotations to the FrozenFrame class."
            )
        if missing := declared - provided:
            raise SchemaValidationError(
                f"Missing key(s) required by schema: {sorted(missing)}. "
                "Ensure all declared fields are present in the data."
            )

        arrays: list[pa.Array] = []
        for f in schema:
            try:
                arrays.append(pa.array(data[f.name], type=f.type))
            except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
                raise SchemaValidationError(
                    f"Column '{f.name}': could not convert data to {f.type!r}. {exc}"
                ) from exc

        batch = pa.record_batch(arrays, schema=schema)

        if validate:
            _validate_batch(schema, batch)

        return cls._from_batch(batch)

    @classmethod
    def from_arrow(
        cls,
        batch: pa.RecordBatch,
        *,
        validate: bool = True,
    ) -> Self:
        """Construct from an existing ``pa.RecordBatch``.

        Parameters
        ----------
        batch:
            An Arrow RecordBatch whose schema must match the declared schema.
        validate:
            Run schema validation.  Defaults to ``True``.

        Returns
        -------
        Self
            A new instance of the concrete subclass.

        Raises
        ------
        SchemaValidationError
            If the batch does not conform to the declared schema.
        """
        if validate:
            _validate_batch(cls.__schema__, batch)
        return cls._from_batch(batch)

    # ------------------------------------------------------------------
    # Immutability guards
    # ------------------------------------------------------------------

    def __setattr__(self, name: str, value: Any) -> None:
        raise FrozenFrameError(
            f"{type(self).__name__!r} is immutable — cannot set attribute '{name}'."
        )

    def __delattr__(self, name: str) -> None:
        raise FrozenFrameError(
            f"{type(self).__name__!r} is immutable — cannot delete attribute '{name}'."
        )

    def __setitem__(self, key: str, value: Any) -> None:
        raise FrozenFrameError(
            f"{type(self).__name__!r} is immutable — cannot set column '{key}'."
        )

    def __delitem__(self, key: str) -> None:
        raise FrozenFrameError(
            f"{type(self).__name__!r} is immutable — cannot delete column '{key}'."
        )

    # ------------------------------------------------------------------
    # Column access
    # ------------------------------------------------------------------

    def __getitem__(self, key: str) -> pa.Array:
        """Return the column ``key`` as a ``pa.Array``.

        Will be upgraded to return ``FrozenSeries[T]`` when that class
        is implemented.
        """
        schema = type(self).__schema__
        if key not in schema.names:
            raise KeyError(
                f"Column '{key}' not found. Available columns: {schema.names}"
            )
        return self._batch.column(key)

    def __getattr__(self, name: str) -> pa.Array:
        """Attribute-style column access: ``df.score``.

        Only called when normal attribute lookup fails, so internal
        attributes (``_batch``, ``__schema__``, …) are unaffected.
        """
        # Guard against calls before _batch is initialised (e.g. during
        # unpickling or repr of a partially-constructed object).
        if "_batch" not in self.__dict__:
            raise AttributeError(name)
        schema = type(self).__schema__
        if name in schema.names:
            return self._batch.column(name)
        raise AttributeError(
            f"'{type(self).__name__}' has no attribute '{name}'. "
            f"Available columns: {schema.names}"
        )

    # ------------------------------------------------------------------
    # Sequence protocol
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        """Return the number of rows."""
        return len(self._batch)

    def __iter__(self) -> typing.Iterator[dict[str, Any]]:
        """Iterate row-wise, yielding each row as a plain Python dict."""
        batch = self._batch
        names = type(self).__schema__.names
        for i in range(len(batch)):
            yield {name: batch.column(name)[i].as_py() for name in names}

    # ------------------------------------------------------------------
    # Equality and hashing
    # ------------------------------------------------------------------

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FrozenFrame):
            return NotImplemented
        if type(self).__schema__ != type(other).__schema__:
            return False
        return self._batch.equals(other._batch)

    def __hash__(self) -> int:
        """Stable hash based on the Arrow IPC serialisation of the batch.

        Computed once on first call and cached internally.
        """
        try:
            return object.__getattribute__(self, "_hash_cache")
        except AttributeError:
            buf = io.BytesIO()
            with pa.ipc.new_stream(buf, self._batch.schema) as writer:
                writer.write_batch(self._batch)
            digest = hashlib.sha256(buf.getvalue()).digest()
            h = int.from_bytes(digest[:8], "big")
            object.__setattr__(self, "_hash_cache", h)
            return h

    # ------------------------------------------------------------------
    # Display
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        schema = type(self).__schema__
        col_summary = ", ".join(f"{f.name}: {f.type}" for f in schema)
        return (
            f"{type(self).__name__}("
            f"{len(self._batch)} rows x {len(schema)} cols"
            f" | {col_summary})"
        )
