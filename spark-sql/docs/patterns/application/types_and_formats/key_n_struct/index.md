# :material-key-variant: Keys & Structs

Check for dynamic key existence in MAP columns using MAP_ENTRIES and EXISTS on Databricks system tables.

---

## :material-sitemap: Overview

```mermaid
graph LR
    A[MAP column] --> B[MAP_ENTRIES]
    B --> C[EXISTS check]
    C --> D[Filtered rows]
```

---

## :material-pin: Quick Reference

| Technique | Use Case | Key Function |
|-----------|----------|-------------|
| MAP_ENTRIES | Iterate over map key-value pairs as array of structs | `MAP_ENTRIES(map_col)` |
| EXISTS(array, pred) | Test if any element matches a predicate | `EXISTS(arr, x -> condition)` |
| FILTER | Keep only matching map entries | `FILTER(MAP_ENTRIES(col), ...)` |

---

## :material-magnify: Examples

### Any Key Exists in Struct

Check whether any key in a MAP column matches a runtime predicate — demonstrated on the Databricks `system.billing.usage` system table.

```sql
--8<-- "sql/application/key_n_struct/any_key_exists_in_struct.sql"
```

---

## :material-brain: When to Use

| Scenario | Recommended Approach |
|----------|---------------------|
| Check if any key exists in a map | `EXISTS` + `MAP_ENTRIES` |
| Databricks system tables with MAP columns | `system.billing.usage` pattern |
| Dynamic key check without knowing key name | Runtime predicate with `EXISTS` |

!!! note
    MAP_ENTRIES returns an array of structs. Combine with EXISTS to check dynamic keys without knowing the key name at query-write time.
