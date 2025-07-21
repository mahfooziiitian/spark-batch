# ✅ 2. sha1(expr), sha2(expr, bitLength)

SHA1: 160-bit hash

SHA2: Choose between 224, 256, 384, or 512 bits

```sql
SELECT sha1('Databricks') AS sha1_hash;
SELECT sha2('Databricks', 256) AS sha2_hash;
```
