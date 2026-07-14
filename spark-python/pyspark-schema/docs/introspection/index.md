# Schema Introspection

After loading or defining a schema, PySpark provides several methods to
inspect and serialise it.

| Method | Returns | Description |
| ------ | ------- | ----------- |
| `df.printSchema()` | `None` (side-effect) | Human-readable tree printed to stdout |
| `df.schema` | `StructType` | Full schema object |
| `df.schema.simpleString()` | `str` | Compact DDL-like string |
| `df.schema.json()` | `str` | JSON string (full metadata) |
| `df.schema.jsonValue()` | `dict` | Python dict (same content as `.json()`) |
| `df.schema.printTreeString()` | `None` | Same as `printSchema` |
| `df.dtypes` | `list[tuple]` | `[(name, type_str), …]` |
| `df.columns` | `list[str]` | Column names only |
| `df.schema.fieldNames()` | `list[str]` | Same as `df.columns` |
| `df.schema.fields` | `list[StructField]` | Full field objects |

See the detailed pages:

- [Print Schema](print-schema.md) — output formats and nested field navigation
- [Column Existence](column-existence.md) — checking presence of top-level and nested columns
