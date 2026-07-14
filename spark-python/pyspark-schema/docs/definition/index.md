# Schema Definition

PySpark offers four distinct ways to define a `StructType`. Choose the one
that best fits your workflow.

| Style | API | Best For |
| ----- | --- | -------- |
| [StructField list](struct-field-list.md) | `StructType([StructField(…), …])` | Full explicit control over every field |
| [Builder](builder.md) | `StructType().add(…).add(…)` | Incremental / programmatic construction |
| [DDL & MapType](ddl-map-type.md) | `StructType.fromDDL("…")` | Hive-compatible DDL, `MapType` |
| [From JSON](from-json.md) | `StructType.fromJson(dict)` | Schema registry, external config |
| [Decimal Type](decimal-type.md) | `DecimalType(precision, scale)` | Financial data, exact arithmetic |

```mermaid
graph LR
    A["StructField list\nStructType([…])"]   -->|most explicit| S[StructType]
    B[".add() builder\nfluent API"]          -->|incremental|   S
    C["DDL string\nfromDDL(…)"]             -->|Hive syntax|   S
    D["JSON dict\nfromJson(…)"]             -->|roundtrip|     S
    S --> P["printSchema / simpleString / json"]
```

All four styles produce identical `StructType` objects and can be freely mixed.
