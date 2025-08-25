# Replace Map Key

> **Note:** Map entry order is not guaranteed in output; results below are conceptual.

---

## Simple Remap (No Collisions)

- **key_map:** `A → X`, `B → Y`
- **original_map:** `{A: 1, B: 2, C: 3}`
- **Expected:** `{X: 1, Y: 2, C: 3}`

---

## Collision with Existing Original Key

- **key_map:** `A → X`
- **original_map:** `{A: 1, X: 9}`
- Remapping `A → X` would collide with existing key `X`; the rule skips A’s remap.
- **Expected:** `{X: 9}` (`A: 1` is dropped due to collision skip)

---

## Two Keys Map to the Same New Key (Dedupe Keeps Last)

- **key_map:** `A → Z`, `B → Z`
- **original_map:** `{A: 1, B: 2}`
- Both remap to `Z`. The second aggregate + filter keeps the last of the duplicates.
- **Expected:** `{Z: 2}` (assuming iteration A then B; last wins)

---

## Identity Mapping (No-op) Plus One Real Remap

- **key_map:** `A → A`, `B → B`, `C → K`
- **original_map:** `{A: 10, B: 20, C: 30}`
- **Expected:** `{A: 10, B: 20, K: 30}`

---

## key_map Has Entries for Missing Keys (Ignored)

- **key_map:** `Z → Q`, `C → K`
- **original_map:** `{A: 1, B: 2}`
- **Expected:** `{A: 1, B: 2}`

---

## Empty key_map

- **original_map:** `{A: foo, B: bar}`
- **Expected:** `{A: foo, B: bar}`

---

## Empty original_map

- **Expected:** `{}`

---

## Whitespace Normalization

- **key_map:** `' Tenant' → 'Tenant'`, `'Tent' → 'Tenant'`, `'Environ' → 'Env'`
- **original_map:** `{' Tenant': 1, 'c': 3, 'Environ': 'prod'}`
- **Expected:** `{Tenant: 1, c: 3, Env: 'prod'}`

---

## Case Sensitivity

- **key_map:** `env → ENV`, `Prod → PROD`
- **original_map:** `{Env: 1, env: 2, Prod: 3}`
- Only exact-case matches remap: `env → ENV`, `Prod → PROD`; `Env` stays.
- **Expected:** `{Env: 1, ENV: 2, PROD: 3}`

---

## Null Value Supported

- **key_map:** `A → X`
- **original_map:** `{A: null, B: 'bbb'}`
- **Expected:** `{X: null, B: 'bbb'}`

---

## Mixed Complex Scenario

- **key_map:** `U → X` (collides), `A → Z`, `B → Z` (duplicate target)
- **original_map:** `{U: 100, X: 999, A: 10, B: 20, C: 30}`
- `U → X` collides with existing key `X` → skip U’s remap; `A → Z` and `B → Z` both target `Z` → keep last (`B`).
- **Expected:** `{X: 999, Z: 20, C: 30}`

---

## Single-hop Only (No Chaining)

- **key_map:** `A → B`, `B → C`
- **original_map:** `{A: 1, B: 2, C: 3}`
- `A` maps to `B` (not then to `C`), `B` maps to `C`, `C` stays `C`.
- There’s a collision potential when `A → B` and original already has `B`; since `B` exists, `A`’s remap is skipped (collision rule).
- `B → C` proceeds (but original key `C` exists, so this is a collision and is skipped).
- Final: only original keys remain untouched.
- **Expected:** `{C: 3}`

> This test highlights a subtle behavior: colliding remaps get dropped, not preserved as original keys.

---
> 🔎 **Behavior summary of your query**
>
> - For each `(original_key, value)`:
>   - Compute `new_key = coalesce(key_map[original_key], original_key)`.
>   - If `new_key` **already exists** among the **original_map's keys** and `new_key != original_key`, then **skip** (to avoid overriding original).  
>   - Otherwise, include `(new_key, value)`.
> - If multiple entries produce the **same `new_key`**, the final reduction keeps the **last** one (based on iteration order of `map_entries()`).

---

## Tips & Variations

- If you **want to preserve** the original `(original_key, value)` when a collision occurs (instead of dropping it), you’d need to tweak the first aggregate to append the **original** pair instead of skipping.
- If you want **multi-hop remapping** (A→B and then B→C), you’ll need an iterative/recursive approach or a loop of remap passes until no change.

---

If you’d like, I can tailor the tests to your exact dataset or adjust the logic (e.g., “preserve-on-collision” behavior). Do you want these as a **Databricks SQL notebook cell** with a pretty display of each `final_map` by `test_id`?

---