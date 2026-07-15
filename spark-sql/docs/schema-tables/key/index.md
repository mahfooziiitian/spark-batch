# :material-key: Keys in Spark SQL

Keys uniquely identify rows and define relationships between tables. In Spark SQL and
Delta Lake, key constraints are **informational** (not enforced at write time) — the
application pipeline is responsible for maintaining uniqueness and referential integrity.

---

## :material-sitemap: In This Section

| Page | Covers |
|------|--------|
| [Primary Keys](primary_key.md) | Declaring, enforcing, and querying primary keys |
| [Foreign Keys](foreign_key.md) | Referential integrity declarations and join patterns |
| [Surrogate Keys](surrogate_key.md) | Auto-increment, UUID, hash-based surrogate key generation |
| [Composite Keys](composite_key.md) | Multi-column keys, uniqueness checks, join patterns |
| [Natural Keys](natural_key.md) | Business keys, upsert patterns, key normalization |
| [Key Constraints in Delta](delta_constraints.md) | `NOT NULL`, `CHECK`, and informational key constraints |

---

## :material-code-tags: Key Types at a Glance

| Key Type | Uniqueness | Enforced in Spark/Delta | Typical Column |
|----------|-----------|------------------------|----------------|
| Primary key | Must be unique + NOT NULL | Informational only | `customer_id BIGINT` |
| Foreign key | References PK of another table | Informational only | `order.customer_id` |
| Surrogate key | Unique, system-generated | Via pipeline | `BIGINT GENERATED ALWAYS AS IDENTITY` |
| Composite key | Unique across N columns | Via pipeline | `(order_id, line_id)` |
| Natural key | Unique business identifier | Via pipeline | `email`, `sku`, `tax_id` |

---

## :material-information-outline: Spark SQL Key Behavior

1. **Primary and foreign key constraints** declared with `CONSTRAINT ... PRIMARY KEY` or `FOREIGN KEY` are stored in the metastore but **not enforced** — Spark will not raise errors for duplicates or referential violations.
2. **Delta `NOT NULL`** is the only constraint that is **actually enforced** at write time.
3. **Delta `CHECK` constraints** are also enforced — they can encode uniqueness rules indirectly (e.g., `CHECK (customer_id > 0)`).
4. Key uniqueness must be maintained by the pipeline: deduplicate before `INSERT`, use `MERGE` for upserts.
5. The optimizer uses declared `PRIMARY KEY` and `UNIQUE` constraints as hints for join planning and predicate pushdown — declaring them even without enforcement can improve query performance.

!!! note "Databricks Unity Catalog"
    Unity Catalog supports informational primary key and foreign key declarations for
    data lineage, documentation, and BI tool integration — but does **not** enforce them
    at write time.
