# Encryption and Hash Functions

Spark SQL includes functions for encryption, hashing, and masking sensitive
values.

---

## 📌 Common Functions

| Function | Purpose |
|----------|---------|
| `MD5`, `SHA1`, `SHA2` | Hash values |
| `AES_ENCRYPT` | Encrypt with AES |
| `AES_DECRYPT` | Decrypt AES data |
| `BASE64` / `UNBASE64` | Encode / decode |
| `MASK` | Obfuscate sensitive fields |

---

## 🧪 Example

```sql
SELECT SHA2(email, 256) AS email_hash FROM users;
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Anonymize identifiers | Use hash functions |
| Protect PII | Use `MASK` |
| Secure payloads | Use AES encrypt/decrypt |
