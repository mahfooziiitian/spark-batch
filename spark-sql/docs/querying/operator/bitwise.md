# :material-gate-and: Bitwise Operators

Bitwise operators work on the binary representation of integral values. In Spark SQL 4.2, they support integer families such as `TINYINT`, `SMALLINT`, `INT`, and `BIGINT`, but not decimal or floating-point operands.

## :material-animation-play: Interactive Visualization — Bit Mask Playground

<div id="viz-operator-bitwise-mask" class="ts-viz"></div>

Switch operators to see the same inputs rendered in binary and verify how Spark 4.2 computes each result.

<script src="../../../assets/js/querying-operator-viz.js"></script>

______________________________________________________________________

## :material-code-tags: Syntax

| Operator       | Name           | Verified example           | Verified result |
| -------------- | -------------- | -------------------------- | --------------- |
| `&`            | Bitwise AND    | `12 & 10`                  | `8`             |
| `\|`           | Bitwise OR     | `12 \| 10`                 | `14`            |
| `^`            | Bitwise XOR    | `12 ^ 10`                  | `6`             |
| `~`            | Bitwise NOT    | `~12`                      | `-13`           |
| `<<`           | Left shift     | `1 << 3`                   | `8`             |
| `>>`           | Right shift    | `16 >> 2`                  | `4`             |
| `bit_count(n)` | Count set bits | `bit_count(7)`             | `3`             |
| `bit_and(col)` | Aggregate AND  | `bit_and(x)` over `(3, 1)` | `1`             |
| `bit_or(col)`  | Aggregate OR   | `bit_or(x)` over `(3, 1)`  | `3`             |
| `bit_xor(col)` | Aggregate XOR  | `bit_xor(x)` over `(3, 1)` | `2`             |

______________________________________________________________________

## :material-information-outline: Verified Behavior

- `12 & 10 = 8`, `12 | 10 = 14`, `12 ^ 10 = 6`, and `~12 = -13` in PySpark 4.2.
- Mixed integral inputs are widened as needed: `typeof(CAST(7 AS TINYINT) & CAST(3 AS SMALLINT))` is `smallint`.
- `NULL` propagates through bitwise expressions: `5 & NULL`, `5 | NULL`, `5 ^ NULL`, and `~CAST(NULL AS INT)` all return `NULL`.
- Decimal / floating-point operands are rejected during analysis. `SELECT 5.0 & 1` fails with `DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES`.
- Shift operators are arithmetic integer shifts, not string or binary transforms.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Flags inside an integer bitmask

```sql
SELECT
    user_id,
    permissions,
    (permissions & 1) = 1 AS can_read,
    (permissions & 2) = 2 AS can_write,
    (permissions & 4) = 4 AS can_execute,
    (permissions & 8) = 8 AS is_admin
FROM user_permissions;
```

### Set or clear a flag

```sql
-- Set WRITE (bit value 2)
SELECT permissions | 2 AS granted_permissions
FROM user_permissions;

-- Clear WRITE
SELECT permissions & ~2 AS revoked_permissions
FROM user_permissions;
```

### Shift operators

```sql
SELECT 1 << 3 AS left_shifted, 16 >> 2 AS right_shifted;
-- Verified results: 8 and 4
```

### Aggregate bitwise functions

```sql
SELECT
    bit_and(x) AS all_bits_shared,
    bit_or(x) AS any_bit_seen,
    bit_xor(x) AS odd_occurrence_bits
FROM VALUES (3), (1) AS t(x);
```

### Unsupported decimal / floating-point input

```sql
SELECT 5.0 & 1;
-- Analysis error in Spark 4.2: DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES
```

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                        | Pattern                |
| ------------------------------- | ---------------------- |
| Check whether a flag is present | `(mask & flag) = flag` |
| Set a flag                      | \`mask                 |
| Clear a flag                    | `mask & ~flag`         |
| Count enabled bits              | `bit_count(mask)`      |
| Combine flags across rows       | `bit_or(mask)`         |

!!! note

    Bitwise operators are ideal for compact flag columns, but they are much harder to read than explicit boolean columns. Document the flag meanings next to the query or in table metadata.
