# ✅ 4. mask() Functions (For Data Redaction)

While not encryption per se, data masking is often used for privacy and obfuscation in sensitive datasets.

Examples:

```sql
-- Mask all but last 4 digits of phone
SELECT mask('123-456-7890', '###-###-####') AS masked_phone;

-- Use predefined masks
SELECT
  mask_first_n('john.doe@example.com', 2) AS masked_email,
  mask_last_n('4111111111111111', 4) AS masked_card;
```
