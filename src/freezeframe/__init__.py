"""
freezeFrame — Immutable, schema-typed DataFrames backed by Apache Arrow.

Basic usage::

    from freezeframe import FrozenFrame, field

    class UserMetrics(FrozenFrame):
        user_id: int
        name:    str
        score:   float
        active:  bool | None  # nullable

    df = UserMetrics.from_dict({
        "user_id": [1, 2, 3],
        "name":    ["alice", "bob", "carol"],
        "score":   [9.1, 7.4, 8.8],
        "active":  [True, False, None],
    })

``field()`` is only needed when overriding the default Arrow type or adding
column documentation::

    import pyarrow as pa
    from freezeframe import FrozenFrame, field

    class Events(FrozenFrame):
        user_id: int
        score:   float = field(arrow_type=pa.float32())
        label:   str   = field(description="human-readable event label")
"""

from freezeframe.column import field, register_type
from freezeframe.exceptions import FrozenFrameError, SchemaValidationError
from freezeframe.frame import FrozenFrame
from freezeframe.series import FrozenSeries

__version__ = "0.1.0"

__all__: list[str] = [
    "FrozenFrame",
    "FrozenFrameError",
    "FrozenSeries",
    "SchemaValidationError",
    "__version__",
    "field",
    "register_type",
]
