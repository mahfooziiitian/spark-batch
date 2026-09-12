# :material-shield-lock: HEX / UNHEX

`HEX` converts bytes into hexadecimal text, and `UNHEX` converts hexadecimal text back into
raw bytes. This is **encoding**, not hashing: the transformation is reversible.

______________________________________________________________________

## :material-sitemap: Overview

```mermaid
graph LR
    A[Text or Bytes] --> B[HEX]
    B --> C[Hex String]
    C --> D[UNHEX]
    D --> E[Original Bytes]
```

### :material-animation-play: Interactive Visualization — UTF-8 Bytes Round-Trip

<div id="viz-hex-roundtrip" class="ts-viz"></div>

Switch between an ASCII string and a multi-byte character to see the underlying UTF-8 bytes.
This makes it clear why some characters expand to more than one hex pair.

______________________________________________________________________

## :material-pin: Syntax

```sql
hex(expr)
unhex(hex_str)
```

- `hex(expr)`: accepts `STRING`, `BINARY`, or `BIGINT`; returns a hex `STRING`
- `unhex(hex_str)`: accepts a hex `STRING`; returns `BINARY`

______________________________________________________________________

## :material-information-outline: Behavior

1. `HEX` on `STRING` inputs encodes the value's underlying bytes as hexadecimal text.
2. `HEX` on numeric inputs returns the number's hexadecimal representation.
3. `UNHEX` reverses hex encoding and returns raw bytes; cast to `STRING` when the bytes represent text.
4. `UNHEX` returns `NULL` for invalid hex strings.
5. **String input is byte-oriented, not character-oriented** — Spark encodes the string as UTF-8 first, so non-ASCII characters may occupy multiple bytes and therefore multiple hex pairs.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### :material-toy-brick: 1. Encode a String to Hex

```sql
SELECT hex('abc') AS hex_value;
-- Result: 616263
```

### :material-toy-brick: 2. Decode Hex Back to Text

```sql
SELECT CAST(unhex('616263') AS STRING) AS original;
-- Result: abc
```

### :material-toy-brick: 3. Numeric to Hex

```sql
SELECT hex(255) AS hex_num;
-- Result: FF
```

### :material-toy-brick: 4. Round-Trip Verification

```sql
SELECT CAST(unhex(hex('Spark SQL')) AS STRING) AS roundtrip;
-- Result: Spark SQL
```

### :material-alert-circle-outline: 5. UTF-8 Multi-Byte Characters Use Multiple Hex Pairs

```sql
SELECT
  hex('é') AS utf8_hex,
  CAST(unhex(hex('é')) AS STRING) AS roundtrip;

-- utf8_hex = C3A9
-- roundtrip = é
```

### :material-toy-brick: 6. Invalid Hex Returns `NULL`

```sql
SELECT unhex('GG') AS invalid_bytes;
-- Result: NULL
```

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                               | Why `HEX` / `UNHEX`?                          |
| -------------------------------------- | --------------------------------------------- |
| Inspect binary values during debugging | Hex text is easier to read than raw bytes     |
| Move bytes through text-only systems   | Hex is portable and reversible                |
| Verify byte-level round-trips          | `UNHEX(HEX(...))` restores the bytes          |
| Investigate encoding problems          | The hex output exposes the real byte sequence |
