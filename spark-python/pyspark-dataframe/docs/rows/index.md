# Rows — Overview

`pyspark.sql.Row` is a namedtuple-like object that serves **two roles**:

1. **Data container** — each collected DataFrame row is a `Row` instance with named fields.
2. **Factory** — `Row("field1", "field2")` returns a reusable subclass for creating typed rows.

## Row Lifecycle

```mermaid
graph LR
    C[Create] -->|"Row(id=1, …)"| A[Access]
    A -->|"row.name / row[0]"| M[Mutate]
    M -->|"withColumn / rdd.map"| V[Convert]
    V -->|"asDict / JSON / tuple"| O[Output]
    style C fill:#4caf50,color:#fff
    style A fill:#2196f3,color:#fff
    style M fill:#ff9800,color:#fff
    style V fill:#9c27b0,color:#fff
    style O fill:#607d8b,color:#fff
```

## Key Characteristics

| Property | Detail |
|----------|--------|
| Immutable | Fields cannot be changed after creation — "mutations" produce a new `Row` |
| Ordered | Fields maintain insertion order; accessible by index or name |
| Hashable | Named-keyword Rows can be used in sets and as dict keys |
| Iterable | `for value in row` yields field values in schema order |
| `__fields__` | Tuple of field names (named Rows only) |
| `asDict()` | Convert to `dict`; pass `recursive=True` for nested structs |

## Pages in This Section

| Page | Content |
|------|---------|
| [Creation](creation.md) | Named kwargs, positional, factory subclass, from dict, nullable, from RDD |
| [Access](access.md) | Attribute access, index, `asDict()`, `hasattr`, `getattr`, `__fields__`, iteration |
| [Conversion](conversion.md) | Row ↔ dict, Row ↔ tuple, JSON round-trip, null normalisation, flatten |
| [Nested](nested.md) | Nested Row, StructType with nested StructField, array of structs, `asDict(recursive=True)` |
| [RDD Map](rdd_map.md) | `map`, `flatMap`, `filter`, `mapPartitions`, `keyBy` over rows |
| [UDF](udf.md) | UDF returning StructType, Row construction inside UDF, `F.struct()` inline |
| [Mutation](mutation.md) | Add / remove / update / rename / merge fields via `withColumn`, `drop`, RDD map |

!!! tip "DataFrame API first"
    Prefer the DataFrame API (`withColumn`, `select`, `drop`) over RDD-level row
    manipulation. The DataFrame API stays inside the Catalyst optimizer and benefits
    from code generation — RDD `map` falls back to interpreted Python execution.

!!! note "Row vs namedtuple"
    `Row` behaves like a `collections.namedtuple` but is **not** a subclass of it.
    It supports both attribute access (`row.name`) and dict-style access (`row["name"]`),
    which standard namedtuples do not.
