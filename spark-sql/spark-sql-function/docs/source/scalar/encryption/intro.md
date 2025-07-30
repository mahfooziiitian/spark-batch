# Introduction

## 🛡️ Recommended Practice for Security

Use Case |Recommended Function
----|----
Data obfuscation (PII)| mask(), mask_first_n()
Hashing for consistency checks |md5(), sha2()
Token masking |crc32()
Encoding sensitive strings |hex() + base64()

## 🔒 Are AES/RSA or Full Encryption Functions Supported?

As of now:

Symmetric (AES) or asymmetric (RSA) encryption/decryption is not supported natively in SQL on Databricks.

For full encryption, you'd use:

User-defined functions (UDFs) in Python/Scala

Key management with Databricks secrets + notebook logic

Delta Sharing or Unity Catalog for access control, not encryption logic
