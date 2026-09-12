# :material-shield-lock: Encryption, Hashing, Masking, and Hex Encoding

Spark SQL groups several related-looking functions under "encryption," but they solve
**different problems**: checksums detect accidental changes, hashes create stable
fingerprints, masking redacts readable text, and hex encoding turns bytes into text-safe
characters.

______________________________________________________________________

## :material-sitemap: Overview

| Function                                          | Category           | Reversible     | Typical Use                                            |
| ------------------------------------------------- | ------------------ | -------------- | ------------------------------------------------------ |
| [`AES_ENCRYPT`](aes.md) / [`AES_DECRYPT`](aes.md) | Encryption         | Yes (with key) | Recoverable column-level encryption of PII and secrets |
| `CRC32(expr)`                                     | Checksum           | No             | Fast change detection and accidental-corruption checks |
| `MD5(expr)`                                       | Hash               | No             | Non-adversarial fingerprints and dedup keys            |
| `SHA1(expr)` / `SHA2(...)`                        | Cryptographic hash | No             | Stronger pseudonymous IDs and integrity digests        |
| `HEX(expr)` / `UNHEX(expr)`                       | Byte encoding      | Yes            | Inspecting or transporting binary data                 |
| `MASK(expr, ...)`                                 | Redaction          | No             | Hiding PII in analyst-facing outputs                   |

### :material-animation-play: Interactive Visualization — Picking the Right Output Shape

<div id="viz-encryption-overview" class="ts-viz"></div>

Click **Hash**, **Mask**, or **Hex** to see how the *same* source value becomes three very
different kinds of output. The key lesson is that these functions are not interchangeable:
one protects joins, one protects eyeballs, and one simply changes representation.

______________________________________________________________________

## :material-information-outline: Behavior

1. `CRC32`, `MD5`, `SHA1`, and `SHA2` are **deterministic** — the same input produces the same output.
2. `HEX` / `UNHEX` are **reversible** byte encoders, not security features.
3. `MASK` preserves a recognizable shape, so analysts can still spot formats like emails or phone numbers.
4. `CRC32` returns a numeric checksum; `MD5` / `SHA*` return hexadecimal digest strings.
5. **Choose by goal, not by name** — hashing, masking, and encoding often all look like "scrambling," but they answer different business questions. Use hashing for joins/dedup, masking for display-safe text, and hex when you need a reversible byte representation.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### :material-toy-brick: 1. Quick Side-by-Side Comparison

```sql
SELECT
  crc32('Spark') AS crc32_val,
  md5('Spark') AS md5_hash,
  sha2('Spark', 256) AS sha256_hash,
  hex('Spark') AS hex_encoded,
  mask('555-12-3456') AS masked;

-- crc32_val  = 1557323817
-- md5_hash   = 8cde774d6f7333752ed72cacddb05126
-- sha256_hash= 529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b
-- hex_encoded= 537061726B
-- masked     = nnn-nn-nnnn
```

### :material-toy-brick: 2. The Same Value, Three Different Purposes

```sql
SELECT
  sha2('555-12-3456', 256) AS stable_hash,
  mask('555-12-3456') AS display_mask,
  hex('555-12-3456') AS reversible_hex,
  CAST(unhex(hex('555-12-3456')) AS STRING) AS restored_text;

-- stable_hash  = one-way 64-char digest for joins / pseudonymous IDs
-- display_mask = nnn-nn-nnnn for reports and screenshots
-- reversible_hex can be decoded back to the original text
-- restored_text = 555-12-3456
```

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Need                                          | Best Choice             | Why                                            |
| --------------------------------------------- | ----------------------- | ---------------------------------------------- |
| Detect row changes cheaply                    | `CRC32`                 | Fast, compact checksum                         |
| Build stable surrogate or dedup keys          | `SHA2(_, 256)` or `MD5` | Deterministic fingerprints                     |
| Keep sensitive text readable-but-redacted     | `MASK`                  | Preserves format while hiding character values |
| Inspect binary payloads or round-trip bytes   | `HEX` / `UNHEX`         | Reversible representation                      |
| Security-sensitive digesting                  | `SHA2(_, 256+)`         | Larger cryptographic digest space than `MD5`   |
| Recover the original value later (reversible) | [`AES_ENCRYPT`](aes.md) | Key-based encryption; use `GCM` mode           |

> **Tip:** If someone says "obfuscate this column," clarify whether they need
> a **joinable fingerprint** (`SHA2`), a **report-safe display value** (`MASK`),
> or a **reversible transport encoding** (`HEX`).
