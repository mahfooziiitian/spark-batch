# :material-swap-horizontal: Migration Guide — Spark 3.5 → 4.0

!!! warning "Breaking Changes"
    Spark 4.0 includes several breaking changes from 3.5. Review this guide
    before upgrading.

---

## :material-pin: Key Breaking Changes

### 1. ANSI Mode Enabled by Default

The most impactful change. `spark.sql.ansi.enabled` defaults to **`true`**.

| Behavior | Spark 3.5 | Spark 4.0 |
|----------|-----------|-----------|
| `CAST('abc' AS INT)` | `NULL` | **Error** |
| `2147483647 + 1` | Wraps silently | **Error** |
| Invalid INSERT types | Coerced/NULL | **Error** |

**Fix:** Use `try_cast`, `try_add`, etc. or set `spark.sql.ansi.enabled=false`.

See [ANSI Mode](../config/ansi.md) for details.

### 2. CREATE TABLE Without USING

```sql
-- Spark 3.5: Creates a Hive SerDe table
-- Spark 4.0: Uses spark.sql.sources.default (Parquet by default)
CREATE TABLE t (id INT, name STRING);
```

**Legacy config:** `spark.sql.legacy.createHiveTableByDefault=true`

### 3. Map Key Normalization

```sql
-- Spark 4.0: -0.0 normalized to 0.0 in map keys
SELECT map(-0.0, 'neg_zero');
-- Key becomes 0.0
```

**Legacy config:** `spark.sql.legacy.disableMapKeyNormalization=true`

### 4. `!` as NOT Operator

```sql
-- Spark 3.5: expr ! IN (...) was allowed
-- Spark 4.0: Syntax error (use NOT instead)
SELECT * FROM t WHERE id NOT IN (1, 2, 3);  -- correct
```

**Legacy config:** `spark.sql.legacy.bangEqualsNot=true`

### 5. CTE Precedence

```sql
-- When inner and outer CTEs share a name:
-- Spark 3.5: raised EXCEPTION
-- Spark 4.0: inner definition takes precedence (CORRECTED)
```

**Legacy config:** `spark.sql.legacy.ctePrecedencePolicy=EXCEPTION`

---

## :material-format-list-bulleted: Full Breaking Changes Table

| Change | Spark 3.5 | Spark 4.0 | Legacy Config |
|--------|-----------|-----------|---------------|
| ANSI mode | `false` | `true` | `spark.sql.ansi.enabled=false` |
| CREATE TABLE default | Hive | `sources.default` (Parquet) | `createHiveTableByDefault=true` |
| Map key `-0.0` | Kept as-is | Normalized to `0.0` | `disableMapKeyNormalization=true` |
| `encode()`/`decode()` charsets | Any JDK charset | Limited set only | `spark.sql.legacy.javaCharsets=true` |
| `!` as NOT | Allowed | Syntax error | `bangEqualsNot=true` |
| CTE name conflict | Exception | Inner wins | `ctePrecedencePolicy=EXCEPTION` |
| Time parser policy | Exception | NULL or error | `timeParserPolicy=EXCEPTION` |
| ORC compression | `snappy` | `zstd` | `orc.compression.codec=snappy` |
| PostgreSQL TIMESTAMP | No TZ | With TZ | `postgres.datetimeMapping.enabled=true` |
| MySQL FLOAT | `DoubleType` | `FloatType` | Cast explicitly |
| MySQL SMALLINT | `IntegerType` | `ShortType` | Cast explicitly |
| Timestamp → int overflow | Wraps | `NULL` | N/A |
| View schema compensation | Up-cast only | Full cast | `viewSchemaCompensation=false` |
| Storage-Partitioned Join | Disabled | Enabled | Set to `false` |
| `maxSinglePartitionBytes` | `Long.MAX` | `128m` | Set to old value |

---

## :material-check-all: Migration Checklist

- [ ] **Audit CAST/arithmetic** — replace with `try_*` variants where NULL-on-error is expected
- [ ] **Check CREATE TABLE** statements — add explicit `USING PARQUET` or `USING DELTA`
- [ ] **Test map operations** with `-0.0` keys
- [ ] **Replace `!` with `NOT`** in filter expressions
- [ ] **Review ORC pipelines** — default compression changed to `zstd`
- [ ] **Check JDBC connectors** — PostgreSQL timestamp and MySQL type mappings changed
- [ ] **Test CTE-heavy queries** with name conflicts
- [ ] **Verify `encode()`/`decode()`** charset usage
- [ ] Run full test suite with ANSI mode on before deploying

---

## :material-new-box: New Features to Adopt

After migration, take advantage of new Spark 4.0 features:

| Feature | Documentation |
|---------|---------------|
| Pipe Syntax `\|>` | [Pipe Syntax](../pipe/index.md) |
| VARIANT data type | [VARIANT](../types/variant/index.md) |
| String Collation | [Collation](../collation/index.md) |
| SQL UDFs | [SQL UDFs](../function/sql_udf/index.md) |
| Session Variables | [Variables](../variables/index.md) |
| EXECUTE IMMEDIATE | [Execute Immediate](../execute_immediate/index.md) |
| IDENTIFIER clause | [IDENTIFIER](../identifier/index.md) |
| SQL Scripting | [Control Flow](../control/index.md) |
| Lateral Column Alias | [Lateral Alias](../column/lateral_alias.md) |
