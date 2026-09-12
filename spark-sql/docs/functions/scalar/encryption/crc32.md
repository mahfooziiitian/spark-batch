# :material-shield-lock: CRC32

`CRC32` computes a fast 32-bit checksum. It is excellent for **accidental change detection**
in ETL pipelines, but it is **not** a cryptographically secure digest.

______________________________________________________________________

## :material-sitemap: Overview

```mermaid
graph LR
    A[Input Bytes] --> B[CRC32]
    B --> C[32-bit Numeric Checksum]
```

### :material-animation-play: Interactive Visualization — Fast Checksum vs Security Digest

<div id="viz-crc32-vs-sha" class="ts-viz"></div>

Switch between an original and modified payload to see both digests change. The visualization
also contrasts CRC32's compact 32-bit number with SHA2's much larger digest space.

______________________________________________________________________

## :material-pin: Syntax

```sql
crc32(expr)
```

- `expr`: `STRING` or `BINARY` input
- Returns: `BIGINT` containing the unsigned 32-bit CRC value

______________________________________________________________________

## :material-information-outline: Behavior

1. Computes a CRC-32 checksum of the input expression.
2. Useful for **quick integrity checks** and low-cost change detection.
3. Returns a numeric value, not a hexadecimal digest string.
4. `NULL` input returns `NULL`.
5. **Not cryptographically secure** — CRC32's 32-bit output space is small, so collisions are much easier than with `SHA2`. Use it for accidental corruption, not passwords, signatures, or tamper-resistant auditing.

!!! warning "Security guidance"

    `CRC32` can tell you that data *probably changed*, but it cannot prove a value was not
    maliciously crafted to match some checksum. For security-sensitive hashing, prefer `SHA2`.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### :material-toy-brick: 1. Basic Checksum

```sql
SELECT crc32('Databricks') AS crc_value;
-- Result: 3213672596
```

### :material-toy-brick: 2. Compare Data Integrity

```sql
SELECT
  crc32('hello world') AS hash1,
  crc32('hello world') AS hash2,
  crc32('Hello World') AS hash3;

-- hash1 = hash2 because identical inputs match
-- hash3 differs because CRC32 is case-sensitive
```

### :material-toy-brick: 3. Change Detection in ETL

```sql
CREATE OR REPLACE TEMP VIEW source AS
SELECT * FROM VALUES
  ('Alice', 100),
  ('Bob', 200)
AS source(name, amount);

SELECT
  name,
  amount,
  crc32(CONCAT(name, CAST(amount AS STRING))) AS row_checksum
FROM source;
```

### :material-alert-circle-outline: 4. Use `SHA2` When the Digest Must Resist Tampering

```sql
SELECT
  crc32('invoice-1001|99.95') AS fast_checksum,
  sha2('invoice-1001|99.95', 256) AS secure_digest;

-- fast_checksum = 2849482112
-- secure_digest = cf45ac55cff0c5d32a813c6417e368464c71efaaef0b29d5ab07ec0e0c191666
```

______________________________________________________________________

## :material-lightbulb-outline: CRC32 vs Other Hashes

| Function       | Output Size | Speed   | Best Fit                                          |
| -------------- | ----------- | ------- | ------------------------------------------------- |
| `crc32`        | 32-bit int  | Fastest | Integrity checks, row-change detection            |
| `md5`          | 128-bit hex | Fast    | Legacy fingerprints, dedup keys                   |
| `sha2(_, 256)` | 256-bit hex | Slower  | Security-sensitive or collision-resistant digests |
