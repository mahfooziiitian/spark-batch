# :material-shield-lock: MD5

`MD5` computes a 128-bit digest and returns it as a 32-character hexadecimal string. It is still
useful for **non-adversarial fingerprints** and lightweight dedup keys, but it is not appropriate
for modern security-sensitive hashing.

______________________________________________________________________

## :material-sitemap: Overview

```mermaid
graph LR
    A[Input Value] --> B[MD5]
    B --> C[32-char Hex Digest]
```

### :material-animation-play: Interactive Visualization — Where MD5 Still Fits

<div id="viz-md5-fit" class="ts-viz"></div>

Toggle between a safe fit and a bad fit for MD5. The digest stays deterministic in both cases,
but the acceptability of the algorithm depends on whether an adversary is part of the problem.

______________________________________________________________________

## :material-pin: Syntax

```sql
md5(expr)
```

- `expr`: `STRING` or `BINARY` input
- Returns: lowercase 32-character hex `STRING`

______________________________________________________________________

## :material-information-outline: Behavior

1. Produces a 128-bit digest rendered as 32 lowercase hexadecimal characters.
2. Deterministic — the same input always produces the same output.
3. `NULL` input returns `NULL`.
4. Fast and convenient for fingerprints, dedup keys, and low-cost change detection.
5. **Not cryptographically secure** — collision attacks are practical, so prefer `SHA2` for passwords, signatures, or tamper-sensitive workflows.
6. **Be careful when hashing multi-column rows** — `CONCAT_WS` skips `NULL` inputs, so rows that differ only by missing values can collapse to the same MD5 unless you add explicit null markers.

!!! warning "Security guidance"

    `MD5` is fine for many operational fingerprints, but not for adversarial security.
    If someone can intentionally craft inputs, use `SHA2` instead.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### :material-toy-brick: 1. Basic Hash

```sql
SELECT md5('Databricks') AS md5_hash;
-- Result: 4e3f864ba9cf3c62d471e1c62414d098
```

### :material-toy-brick: 2. Row-Level Hashing for Change Detection

```sql
CREATE OR REPLACE TEMP VIEW records AS
SELECT * FROM VALUES
  (1, 'Alice', 100),
  (2, 'Bob', 200)
AS records(id, name, amount);

SELECT
  id,
  md5(CONCAT_WS('|', name, CAST(amount AS STRING))) AS row_hash
FROM records;
```

### :material-toy-brick: 3. Deduplication Key

```sql
SELECT md5(CONCAT_WS('|', col1, col2, col3)) AS dedup_key, *
FROM source_table;
```

### :material-toy-brick: 4. NULL Input Propagates

```sql
SELECT md5(CAST(NULL AS STRING)) AS null_hash;
-- Result: NULL
```

### :material-alert-circle-outline: 5. `CONCAT_WS` Can Hide NULL Differences Unless You Mark Them

```sql
SELECT
  CONCAT_WS('|', 'A', NULL, 'B') AS skipped_null,
  CONCAT_WS('|', 'A', 'B') AS no_middle,
  md5(CONCAT_WS('|', 'A', NULL, 'B')) = md5(CONCAT_WS('|', 'A', 'B')) AS same_hash,
  md5(
    COALESCE('A', '∅') || '|' ||
    COALESCE(CAST(NULL AS STRING), '∅') || '|' ||
    COALESCE('B', '∅')
  ) AS hash_with_null_marker;

-- skipped_null          = A|B
-- no_middle             = A|B
-- same_hash             = true
-- hash_with_null_marker = c8fe1512b1e2bdf27aca87c7bda45589
```

______________________________________________________________________

## :material-lightbulb-outline: MD5 vs SHA

| Function       | Output Length      | Security | Typical Fit                         |
| -------------- | ------------------ | -------- | ----------------------------------- |
| `md5`          | 32 chars (128-bit) | Weak     | Dedup keys, legacy row fingerprints |
| `sha1`         | 40 chars (160-bit) | Weak     | Legacy compatibility only           |
| `sha2(_, 256)` | 64 chars (256-bit) | Stronger | Recommended default for new work    |
