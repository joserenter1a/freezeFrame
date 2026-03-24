# Defining Schemas

## Class-level type annotations

A `FrozenFrame` schema is a Python class. Each field is a class-level type
annotation. The metaclass reads `__annotations__` at class creation time and
builds the corresponding `pyarrow.Schema`.

```python
from freezeframe import FrozenFrame

class Order(FrozenFrame):
    order_id:   int
    customer:   str
    amount:     float
    shipped:    bool
```

## Nullable columns

By default, all columns are **non-nullable**. A `None` value in a non-nullable
column raises `SchemaValidationError` at construction.

To allow nulls, annotate with `T | None`:

```python
class Order(FrozenFrame):
    order_id:   int
    customer:   str
    amount:     float
    shipped_at: str | None   # nullable — may be None if not yet shipped
```

This is enforced at construction time. There is no silent coercion.

## Python → Arrow type mapping

| Python annotation | Arrow type |
|---|---|
| `int` | `pa.int64()` |
| `float` | `pa.float64()` |
| `str` | `pa.large_utf8()` |
| `bool` | `pa.bool_()` |
| `bytes` | `pa.large_binary()` |
| `datetime.datetime` | `pa.timestamp("us", tz="UTC")` |
| `datetime.date` | `pa.date32()` |
| `datetime.timedelta` | `pa.duration("us")` |
| `Decimal` | `pa.decimal128(38, 18)` |

## Overriding Arrow types with `field()`

When the default mapping isn't precise enough, use `field(arrow_type=...)`:

```python
import pyarrow as pa
from freezeframe import FrozenFrame, field
import datetime

class Event(FrozenFrame):
    event_id:  int
    score:     float = field(arrow_type=pa.float32())
    ts:        datetime.datetime = field(arrow_type=pa.timestamp("ms"))
```

## Registering custom types

Third-party types can be registered globally with `register_type()`:

```python
from freezeframe import register_type
import pyarrow as pa
import numpy as np

register_type(np.float32, pa.float32())
```

Registered types are then available as bare annotations in any `FrozenFrame`
subclass.

## Schema introspection

The compiled `pyarrow.Schema` is available as a class attribute:

```python
Order.__schema__
# schema:
#   order_id: int64 not null
#   customer: large_utf8 not null
#   amount: double not null
#   shipped: bool not null
```
