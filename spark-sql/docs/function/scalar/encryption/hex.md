# hex / unhex

`hex` converts a value to its hexadecimal representation. `unhex` reverses the conversion.

## 📌 Syntax

```sql
hex(expr)
unhex(hex_str)
```

- `hex(expr)`: Accepts `STRING`, `BINARY`, or `BIGINT`; returns hex-encoded `STRING`
- `unhex(hex_str)`: Accepts hex `STRING`; returns `BINARY`

## 🔍 Behavior

1. **String input**: each character is converted to its 2-digit hex ASCII code.
2. **Numeric input**: the number is converted to its hexadecimal representation.
3. `unhex` reverses hex encoding — returns raw bytes (cast to `STRING` for text).
4. `unhex` returns NULL for invalid hex strings.

## 🧪 Practical Examples

### Encode String to Hex

```sql
SELECT hex('abc') AS hex_value;
-- Result: '616263'
```

### Decode Hex Back to String

```sql
SELECT CAST(unhex('616263') AS STRING) AS original;
-- Result: 'abc'
```

### Numeric to Hex

```sql
SELECT hex(255) AS hex_num;
-- Result: 'FF'
```

### Round-Trip Verification

```sql
SELECT CAST(unhex(hex('Spark SQL')) AS STRING) AS roundtrip;
-- Result: 'Spark SQL'
```

### Binary Data Inspection

```sql
SELECT hex(CAST('Hello' AS BINARY)) AS binary_hex;
-- Result: '48656C6C6F'
```

## 🧠 When to Use

| Scenario | Function |
|----------|----------|
| Inspect binary data as readable text | `hex` |
| Store/transmit binary safely as text | `hex` |
| Convert hex-encoded data back to bytes | `unhex` |
| Debug encoding issues | `hex` + `unhex` round-trip |
