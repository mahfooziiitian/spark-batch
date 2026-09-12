# :material-lock: AES Encryption — `aes_encrypt` / `aes_decrypt`

Unlike hashing (`MD5`, `SHA2`) and masking (`MASK`), which are **one-way**, `aes_encrypt`
produces **reversible** ciphertext that `aes_decrypt` can turn back into the original
plaintext with the correct key. Use it for column-level encryption of sensitive fields
(PII, tokens, account numbers) that must be recoverable downstream.

## :material-pin: Syntax

```sql
aes_encrypt(expr, key[, mode[, padding[, iv[, aad]]]])
aes_decrypt(expr, key[, mode[, padding[, aad]]])
try_aes_decrypt(expr, key[, mode[, padding[, aad]]])
```

| Argument  | Notes                                                                              |
| --------- | ---------------------------------------------------------------------------------- |
| `expr`    | Plaintext (`STRING`/`BINARY`) to encrypt, or ciphertext (`BINARY`) to decrypt.     |
| `key`     | Secret key. **Must be exactly 16, 24, or 32 bytes** (AES-128/192/256).             |
| `mode`    | `'GCM'` (default), `'CBC'`, or `'ECB'`.                                            |
| `padding` | `'NONE'` (default for GCM), `'PKCS'` (default/required for CBC & ECB).             |
| `iv`      | Optional initialization vector (GCM/CBC). Omit to let Spark generate a random one. |
| `aad`     | Optional additional authenticated data (GCM only).                                 |

The output of `aes_encrypt` is `BINARY`; wrap it in [`base64()`](../../scalar/text/index.md)
(or [`hex()`](hex.md)) to store or transport it as text.

## :material-flask-outline: Round-Trip

```sql
-- Encrypt, then decrypt back to the original value
SELECT CAST(
    aes_decrypt(
        aes_encrypt('4111-1111-1111-1111', '0000111122223333'),
        '0000111122223333'
    ) AS STRING
) AS pan;
```

```text
+-------------------+
|pan                |
+-------------------+
|4111-1111-1111-1111|
+-------------------+
```

`aes_decrypt` returns `BINARY`; `CAST(... AS STRING)` recovers the original text.

## :material-shield-check: GCM Is Non-Deterministic (Random IV)

!!! success "Verified in Spark 4.2: the same plaintext encrypts to different ciphertext each time"

    With the default `GCM` mode and no explicit `iv`, Spark generates a fresh random
    initialization vector per call, so encrypting the same value twice yields **different**
    ciphertext:

    ```sql
    SELECT base64(aes_encrypt('secret', '0000111122223333')) AS a,
           base64(aes_encrypt('secret', '0000111122223333')) AS b;
    ```

    ```text
    +------------------------------------------------+------------------------------------------------+
    |a                                               |b                                               |
    +------------------------------------------------+------------------------------------------------+
    |ETeGXDw6wN7mB6CtgWqKz7qxYXHglMnBVRzG9RhglPMW2w==|Cx49Od+uBr459xbjBWFfn6E/u+FFPyX+TLl20zmc82akfQ==|
    +------------------------------------------------+------------------------------------------------+
    ```

    This is the desired behavior for security — an attacker cannot tell that two encrypted
    cells hold the same value. It also means **you cannot join, group, or dedup on GCM
    ciphertext**: identical plaintext produces non-identical bytes. If you need
    equality-comparable encrypted keys, that is exactly what deterministic hashing
    ([`SHA2`](sha.md)) is for — not AES-GCM.

!!! failure "`ECB` is deterministic — avoid it for real data"

    ```sql
    SELECT base64(aes_encrypt('secret', '0000111122223333', 'ECB', 'PKCS')) AS a,
           base64(aes_encrypt('secret', '0000111122223333', 'ECB', 'PKCS')) AS b;
    -- a and b are IDENTICAL: jZgQwB+wGTPuAZgTXBM3kA==
    ```

    `ECB` mode encrypts identical plaintext blocks to identical ciphertext, leaking equality
    and structure. Prefer `GCM` (authenticated, random IV) for anything sensitive. `ECB`
    exists only for interop with legacy systems that require it.

## :material-key-variant: Key Length Is Enforced

!!! warning "Key must be 16, 24, or 32 bytes"

    ```sql
    SELECT aes_encrypt('hello', '0000111122223333444455556666');  -- 28-byte key
    ```

    ```text
    [INVALID_PARAMETER_VALUE.AES_KEY_LENGTH] The value of parameter(s) `key` in
    `aes_encrypt` is invalid: expects a binary value with 16, 24 or 32 bytes, but got 28 bytes.
    ```

    Keys of 16 / 24 / 32 bytes select AES-128 / AES-192 / AES-256 respectively — all three
    round-trip correctly. Never hard-code the key in the SQL text as shown here for
    illustration; source it from a [session variable](../../../spark-4/variables/index.md),
    secret scope, or key-management service.

## :material-close-octagon: `try_aes_decrypt` — NULL Instead of Error

!!! success "Verified: a wrong key returns `NULL` rather than aborting the query"

    ```sql
    SELECT try_aes_decrypt(
        aes_encrypt('hello', '0000111122223333'),
        'wrongwrongwrongX'
    ) AS pt;
    ```

    ```text
    +----+
    |pt  |
    +----+
    |NULL|
    +----+
    ```

    Plain `aes_decrypt` throws on a bad key, corrupt ciphertext, or failed GCM
    authentication, aborting the whole job. `try_aes_decrypt` swallows the failure and
    yields `NULL`, which is useful when decrypting a column that may contain a mix of
    keys/versions or occasional bad rows — filter or branch on the `NULL` afterward.

## :material-brain: When to Use

| Need                                                   | Choice                                |
| ------------------------------------------------------ | ------------------------------------- |
| Recoverable column-level encryption of PII/secrets     | `aes_encrypt` / `aes_decrypt` (`GCM`) |
| Decrypt a column that may contain bad/foreign-key rows | `try_aes_decrypt`                     |
| One-way pseudonymous, **joinable** surrogate key       | [`SHA2`](sha.md) — not AES            |
| Hide text but keep it readable for analysts            | [`MASK`](mask.md)                     |
| Interop with a legacy system that mandates ECB         | `aes_encrypt(..., 'ECB', 'PKCS')`     |

!!! note "Reversible vs. irreversible"

    AES is the only **reversible** transform in this section. If downstream never needs the
    original value back, prefer an irreversible [hash](sha.md) or [mask](mask.md) — they
    remove the key-management burden and the risk of the plaintext being recovered.

## :material-link-variant: Related

- [Hashing — `MD5` / `SHA`](sha.md) — one-way, deterministic, joinable fingerprints.
- [`MASK`](mask.md) — format-preserving redaction for analyst outputs.
- [`HEX` / `UNHEX`](hex.md) — byte-to-text encoding, an alternative to `base64` for ciphertext.
- [Session Variables](../../../spark-4/variables/index.md) — a safer place to hold a key than inline SQL.
