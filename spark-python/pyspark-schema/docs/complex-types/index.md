# Complex Types

Spark supports three complex (compound) data types that can be arbitrarily nested.

| Type | API | Description |
| ---- | --- | ----------- |
| `ArrayType` | `ArrayType(elementType)` | Ordered list of same-type elements |
| `MapType` | `MapType(keyType, valueType)` | Key → value dictionary |
| `StructType` | `StructType([StructField…])` | Named, typed, heterogeneous fields |

```mermaid
graph TD
    Root["StructType (record)"]
    Root --> scalar["scalar fields\nid, name, amount"]
    Root --> arr["ArrayType\ntags: array&lt;string&gt;"]
    Root --> map["MapType\nproperties: map&lt;string,string&gt;"]
    Root --> nested["nested StructType\naddress: struct&lt;city,country&gt;"]
```

!!! tip "Arbitrary nesting"
    All three types can be nested inside each other to any depth.
    An `ArrayType` can hold a `StructType`; a `StructType` field can be
    another `StructType`.

See the detailed pages for hands-on examples:

- [Arrays](arrays.md) — primitives, struct arrays, array functions
- [Nested Structs](nested-structs.md) — multi-level nesting, field access
