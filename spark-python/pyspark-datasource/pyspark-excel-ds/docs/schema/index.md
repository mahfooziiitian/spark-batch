# Schema

Excel has no native concept of a Spark schema — column types are always
inferred by pandas from cell contents. `pys_excel` supports both **schema
inference** (fast, convenient) and **explicit schema** (safe, production
default).

| Approach | Page |
|----------|------|
| Providing an explicit `StructType`/DDL schema | [Explicit Schema](explicit-schema.md) |
| Letting pandas/Spark infer types | [Schema Inference](schema-inference.md) |

!!! tip "Recommendation"
    Use explicit schemas for production/scheduled pipelines feeding tables
    (`excel_to_table`, `upsert_table_from_excel`) — Excel source files are
    prone to silent type drift (a blank cell turning an `int` column into a
    `double`, a stray text value breaking numeric inference, etc.). Reserve
    inference for ad-hoc exploration.
