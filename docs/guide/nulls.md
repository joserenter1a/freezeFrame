# Null Handling

## Explicit nullability

In `freezeFrame`, nullability is a property of the column's declared type —
not an inferred runtime behaviour. This is one of the most important correctness
guarantees the library provides.

| Annotation | Nullable? | Arrow nullability |
|---|---|---|
| `score: float` | No | `not null` |
| `score: float \| None` | Yes | nullable |

## Enforcement at construction

Passing a `None` into a non-nullable column raises `SchemaValidationError`
immediately, at the point of construction — not silently downstream:

```python
class Order(FrozenFrame):
    order_id: int
    amount:   float   # non-nullable

# This raises SchemaValidationError:
Order.from_dict({"order_id": [1], "amount": [None]})
```

## Working with nullable columns

```python
class Order(FrozenFrame):
    order_id:   int
    shipped_at: str | None  # nullable

df = Order.from_dict({
    "order_id":   [1, 2, 3],
    "shipped_at": ["2024-01-01", None, "2024-01-03"],
})
```

`FrozenSeries[str | None]` is returned for nullable columns. Arrow's native
null bitmap tracks which values are valid — no sentinel values like `NaN` or
`-1` are used.

## No silent coercion

`freezeFrame` never silently converts `NaN`, empty strings, or other sentinel
values to `None`. What you put in is what the frame stores. Nulls must be
represented as Python `None` at construction time.

This is a deliberate departure from Pandas, which silently promotes integer
columns to float when nulls are introduced, and uses `NaN` as a null sentinel
in ways that frequently cause unexpected behaviour.
