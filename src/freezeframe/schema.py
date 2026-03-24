"""
Schema construction and validation for FrozenFrame.

Public surface (used by frame.py)
----------------------------------
build_schema(cls)         Build a pa.Schema from a FrozenFrame subclass's annotations.
validate(schema, batch)   Validate a pa.RecordBatch against a schema.

Both functions are internal to the library — they are not part of the public API.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from freezeframe.column import field, resolve_arrow_type
from freezeframe.exceptions import SchemaValidationError

__all__: list[str] = []


def build_schema(annotations: dict[str, Any], defaults: dict[str, Any]) -> pa.Schema:
    """Build a ``pa.Schema`` from a FrozenFrame class's annotations and defaults.

    Parameters
    ----------
    annotations:
        The class's ``__annotations__`` dict, as returned by
        ``typing.get_type_hints()`` or ``cls.__annotations__``.
    defaults:
        The class's ``__dict__`` entries that are ``field()`` instances,
        keyed by field name.

    Returns
    -------
    pa.Schema
        The compiled Arrow schema with nullability applied.

    Raises
    ------
    TypeError
        If an annotation cannot be resolved to an Arrow type.
    """
    pa_fields: list[pa.Field] = []

    for name, annotation in annotations.items():
        override: pa.DataType | None = None
        if name in defaults and isinstance(defaults[name], field):
            override = defaults[name].arrow_type

        arrow_type, nullable = resolve_arrow_type(annotation, override=override)
        pa_fields.append(pa.field(name, arrow_type, nullable=nullable))

    return pa.schema(pa_fields)


def validate(schema: pa.Schema, batch: pa.RecordBatch) -> None:
    """Validate a ``pa.RecordBatch`` against the declared schema.

    Checks performed (in order):
    1. No extra columns in the batch that are absent from the schema.
    2. No columns declared in the schema that are missing from the batch.
    3. Arrow type matches for every column.
    4. Nullability constraint — non-nullable columns must have zero null values.

    Parameters
    ----------
    schema:
        The schema produced by ``build_schema()``.
    batch:
        The ``pa.RecordBatch`` to validate.

    Raises
    ------
    SchemaValidationError
        On the first constraint violation found, with a descriptive message.
    """
    declared = {f.name for f in schema}
    present = set(batch.schema.names)

    extra = present - declared
    if extra:
        raise SchemaValidationError(
            f"Unexpected column(s) not declared in schema: {sorted(extra)}. "
            "Remove them or add matching annotations to the FrozenFrame class."
        )

    missing = declared - present
    if missing:
        raise SchemaValidationError(
            f"Missing column(s) required by schema: {sorted(missing)}. "
            "Ensure all declared fields are present in the data."
        )

    for schema_field in schema:
        col = batch.column(schema_field.name)

        if col.type != schema_field.type:
            raise SchemaValidationError(
                f"Column '{schema_field.name}': expected Arrow type "
                f"{schema_field.type!r}, got {col.type!r}. "
                "Use field(arrow_type=...) to override the default mapping "
                "or cast the data before construction."
            )

        if not schema_field.nullable and col.null_count > 0:
            raise SchemaValidationError(
                f"Column '{schema_field.name}' is declared non-nullable "
                f"but contains {col.null_count} null value(s). "
                "Use 'T | None' in the annotation to allow nulls."
            )
