"""
freezeFrame exceptions.

FrozenFrameError      — raised on any attempted mutation of a FrozenFrame.
SchemaValidationError — raised when data does not conform to the declared schema.
"""


class FrozenFrameError(Exception):
    """Raised when a mutation is attempted on an immutable FrozenFrame."""


class SchemaValidationError(Exception):
    """Raised when data provided at construction does not match the declared schema."""
