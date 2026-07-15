# :material-bitwise-and: Bitwise Operators

Bitwise operators work on the binary representation of integer values. They are used
for flag/bitmask fields, permission systems, hash-based bucketing, and low-level
data manipulation.

---

## :material-code-tags: Syntax

| Operator | Name | Example | Result |
|----------|------|---------|--------|
| `&` | Bitwise AND | `12 & 10` | `8` |
| `\|` | Bitwise OR | `12 \| 10` | `14` |
| `^` | Bitwise XOR | `12 ^ 10` | `6` |
| `~` | Bitwise NOT (complement) | `~12` | `-13` |
| `<<` | Left shift | `1 << 3` | `8` |
| `>>` | Right shift | `16 >> 2` | `4` |
| `BIT_AND(col)` | Aggregate bitwise AND | — | AND across all rows |
| `BIT_OR(col)` | Aggregate bitwise OR | — | OR across all rows |
| `BIT_XOR(col)` | Aggregate bitwise XOR | — | XOR across all rows |
| `bit_count(n)` | Count set bits | `bit_count(7)` | `3` |

---

## :material-information-outline: Behavior

1. Bitwise operators work on `INT`, `BIGINT`, `SMALLINT`, and `TINYINT` — not on `FLOAT`, `DOUBLE`, or `DECIMAL`.
2. `~x` (NOT) returns `-(x + 1)` for signed integers due to two's complement representation.
3. Left shift `x << n` multiplies by 2^n (fast powers of 2); right shift `x >> n` divides by 2^n (integer truncation).
4. `NULL` in any bitwise expression propagates `NULL` to the result.
5. `BIT_AND`, `BIT_OR`, `BIT_XOR` are aggregate functions — they operate across rows in a `GROUP BY` or over a window.

---

## :material-flask-outline: Practical Examples

### Check if a specific permission flag is set

```sql
-- permissions is a bitmask: READ=1, WRITE=2, EXECUTE=4, ADMIN=8
SELECT
    user_id,
    permissions,
    (permissions & 1) != 0  AS can_read,
    (permissions & 2) != 0  AS can_write,
    (permissions & 4) != 0  AS can_execute,
    (permissions & 8) != 0  AS is_admin
FROM user_permissions;
```

### Filter users with a specific flag

```sql
-- Users who have WRITE permission (bit 1)
SELECT user_id, username
FROM user_permissions
WHERE (permissions & 2) = 2;

-- Users with BOTH read AND write
SELECT user_id FROM user_permissions
WHERE (permissions & 3) = 3;  -- 3 = READ(1) | WRITE(2)
```

### Grant and revoke permissions with bitwise OR / AND NOT

```sql
-- Grant WRITE permission (set bit 1)
UPDATE user_permissions
SET permissions = permissions | 2
WHERE user_id = 42;

-- Revoke WRITE permission (clear bit 1 using AND NOT)
UPDATE user_permissions
SET permissions = permissions & ~2
WHERE user_id = 42;
```

### Build a bitmask from individual flags

```sql
SELECT
    user_id,
    (CASE WHEN can_read    THEN 1 ELSE 0 END)
  | (CASE WHEN can_write   THEN 2 ELSE 0 END)
  | (CASE WHEN can_execute THEN 4 ELSE 0 END)
  | (CASE WHEN is_admin    THEN 8 ELSE 0 END) AS permissions_mask
FROM user_flags;
```

### Fast powers of two with shifts

```sql
SELECT
    n,
    1 << n AS power_of_two    -- 2^n
FROM (SELECT EXPLODE(SEQUENCE(0, 10)) AS n) AS t;
-- Result: 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024
```

### Hash-based bucketing

```sql
-- Assign rows to one of 16 buckets using the low 4 bits of a hash
SELECT
    customer_id,
    hash(customer_id) & 15 AS bucket   -- 15 = 0b1111 → 16 buckets (0–15)
FROM customers;
```

### Aggregate bitwise OR — combined flags across rows

```sql
-- What is the union of all permissions granted to a role?
SELECT
    role_id,
    BIT_OR(permissions)  AS combined_permissions,
    BIT_AND(permissions) AS common_permissions    -- flags ALL members share
FROM role_members
GROUP BY role_id;
```

### Check even/odd with bitwise AND

```sql
-- Faster than MOD for integer even/odd check
SELECT order_id,
    CASE WHEN order_id & 1 = 0 THEN 'Even' ELSE 'Odd' END AS parity
FROM orders;
```

### XOR for simple checksum / change detection

```sql
-- Row-level XOR fingerprint across integer columns
SELECT
    record_id,
    id ^ version ^ status_code AS row_checksum
FROM records;

-- Aggregate XOR: if any row changed, the result changes
SELECT BIT_XOR(md5_int_hash) AS table_fingerprint FROM (
    SELECT CAST(CONV(SUBSTRING(MD5(CONCAT_WS('|', id, value)), 1, 15), 16, 10) AS BIGINT) AS md5_int_hash
    FROM source_table
) AS hashed;
```

---

## :material-lightbulb-outline: When to Use Bitwise Operators

| Scenario | Pattern |
|----------|---------|
| Permission / feature flag bitmask | `permissions & FLAG = FLAG` |
| Grant a permission | `permissions \| FLAG` |
| Revoke a permission | `permissions & ~FLAG` |
| Power-of-two constant | `1 << n` |
| Hash bucketing (N must be power of 2) | `hash(col) & (N - 1)` |
| Even/odd check | `col & 1 = 0` → even |
| Aggregate union of flags | `BIT_OR(flags)` |
| Aggregate intersection of flags | `BIT_AND(flags)` |

!!! note "Use descriptive constants"
    Bitmask values like `12` are opaque. Define named CTEs or use `CASE WHEN` to make
    each flag's meaning explicit in the query, or document the bit positions in a
    table comment.
