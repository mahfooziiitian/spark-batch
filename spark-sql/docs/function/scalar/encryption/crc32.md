# :material-shield-lock: crc32

`crc32` computes a 32-bit cyclic redundancy check (CRC) value — a fast checksum
used for data integrity validation.

### :material-sitemap: Overview

```mermaid
graph LR
    A[Plain Text] --> B[Hash Function]
    B --> C[Hash Output]
```

## 📌 :material-shield-lock: Syntax

```sql
crc32(expr)
```

- `expr`: `STRING` or `BINARY` input
- Returns: `BIGINT` — 32-bit CRC hash value

## 🔍 :material-shield-lock: Behavior

1. Computes a CRC-32 checksum of the input expression.
2. Useful for **quick data integrity checks**, not cryptographic security.
3. Returns a numeric value (not a hex string like MD5/SHA).
4. NULL input returns NULL.

## 🧪 :material-shield-lock: Practical Examples

### Basic Hash

```sql
SELECT crc32('Databricks') AS crc_value;
-- Result: 1665816603
```

### Compare Data Integrity

```sql
SELECT
  crc32('hello world') AS hash1,
  crc32('hello world') AS hash2,
  crc32('Hello World') AS hash3;
-- hash1 = hash2 (identical inputs), hash3 differs (case-sensitive)
```

### Change Detection in ETL

```sql
CREATE OR REPLACE TEMP VIEW source AS
SELECT * FROM VALUES ('Alice', 100), ('Bob', 200) AS source(name, amount);

SELECT name, amount, crc32(CONCAT(name, CAST(amount AS STRING))) AS row_hash
FROM source;
```

## 🧠 :material-shield-lock: CRC32 vs Other Hashes

| Function | Output Size | Speed | Use Case |
|----------|------------|-------|----------|
| `crc32` | 32-bit int | Fastest | Data integrity, ETL checksums |
| `md5` | 128-bit hex | Fast | General hashing |
| `sha2(_, 256)` | 256-bit hex | Slower | Cryptographic security |
