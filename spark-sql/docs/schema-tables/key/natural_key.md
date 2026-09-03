# :material-tag-outline: Natural Keys

A natural key is a column (or combination of columns) that has **business meaning** and
uniquely identifies a row in the real world — email address, product SKU, tax ID, ISIN
code. Natural keys are used for deduplication, upserts, and change detection pipelines.

---

## :material-information-outline: Behavior

1. Natural keys can change over time (e.g., email address) — always evaluate stability before using a natural key as a join key in a data warehouse dimension.
2. Natural keys are the correct key for **SCD upsert logic** — use them in the `ON` clause of `MERGE` to match incoming source rows to existing target rows.
3. For dimensions that need versioning (SCD Type 2), combine the natural key with a **surrogate key** — the natural key drives MERGE matching; the surrogate key is used for fact table joins.
4. Natural keys from external systems may contain whitespace, mixed case, or encoding variations — normalise them consistently at ingestion.

---

## :material-code-tags: Syntax

```sql
-- Natural key declared as unique (informational)
CREATE TABLE products (
    product_id  BIGINT  NOT NULL,
    sku         STRING  NOT NULL,    -- natural key
    name        STRING,
    category    STRING,
    price       DECIMAL(10, 2)
)
USING DELTA
CONSTRAINT products_pk PRIMARY KEY (product_id),
CONSTRAINT products_sku_uq UNIQUE (sku);

-- Normalise natural key at ingestion
SELECT
    TRIM(UPPER(sku))     AS sku,
    TRIM(name)           AS name,
    category,
    price
FROM raw_products;
```

---

## :material-flask-outline: Practical Examples

### Normalise natural key before loading

```sql
WITH normalised AS (
    SELECT
        TRIM(UPPER(email))       AS email,
        TRIM(name)               AS name,
        TRIM(UPPER(country))     AS country,
        phone
    FROM raw_customers
    WHERE email IS NOT NULL
)
INSERT INTO customers
SELECT email, name, country, phone
FROM normalised
WHERE email NOT IN (SELECT email FROM customers);
```

### Upsert on natural key (email)

```sql
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY updated_at DESC) AS rn
    FROM staging_customers
)
MERGE INTO customers AS t
USING (SELECT * FROM deduped WHERE rn = 1) AS s
    ON TRIM(UPPER(t.email)) = TRIM(UPPER(s.email))    -- normalised match
WHEN MATCHED AND t.name <> s.name OR t.country <> s.country THEN
    UPDATE SET name = s.name, country = s.country, updated_at = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (email, name, country, phone, created_at)
    VALUES (TRIM(UPPER(s.email)), s.name, TRIM(UPPER(s.country)), s.phone, current_timestamp());
```

### Detect conflicting natural keys (same SKU, different data)

```sql
SELECT
    p1.sku,
    p1.name    AS name_v1,
    p2.name    AS name_v2,
    p1.price   AS price_v1,
    p2.price   AS price_v2
FROM products AS p1
JOIN products AS p2
    ON  TRIM(UPPER(p1.sku)) = TRIM(UPPER(p2.sku))
    AND p1.product_id       < p2.product_id;       -- avoid self-match and duplicates
```

### Natural key change detection (for SCD)

```sql
WITH source_hashed AS (
    SELECT
        sku,
        name,
        category,
        price,
        md5(concat_ws('||', name, category, CAST(price AS STRING))) AS row_hash
    FROM staging_products
),
target_hashed AS (
    SELECT
        sku,
        md5(concat_ws('||', name, category, CAST(price AS STRING))) AS row_hash
    FROM dim_products
    WHERE is_current = TRUE
)
SELECT s.sku, s.name, s.category, s.price
FROM source_hashed AS s
JOIN target_hashed AS t
    ON s.sku = t.sku
WHERE s.row_hash <> t.row_hash;     -- changed rows only
```

### Natural key resolution — map to surrogate key

```sql
-- Resolve SKU (natural key) to product_sk (surrogate key) for fact loading
SELECT
    f.order_id,
    f.order_date,
    dp.product_sk,       -- surrogate key for the fact table FK
    f.quantity,
    f.revenue
FROM staging_fact AS f
JOIN dim_products AS dp
    ON  TRIM(UPPER(dp.sku)) = TRIM(UPPER(f.sku))
    AND dp.is_current = TRUE;
```

### Audit: natural keys that span multiple surrogate keys (SCD check)

```sql
-- For SCD Type 2: each SKU should have exactly one is_current = TRUE row
SELECT sku, COUNT(*) AS current_versions
FROM dim_products
WHERE is_current = TRUE
GROUP BY sku
HAVING current_versions > 1;
```

### Validate natural key uniqueness after load

```sql
-- email must be unique across all active customers
SELECT email, COUNT(*) AS cnt
FROM customers
WHERE is_active = TRUE
GROUP BY email
HAVING cnt > 1;
```

---

## :material-play-circle-outline: Worked Example

Self-contained — the natural key is `email`, but raw values differ only by case and
whitespace. Normalising first reveals they are the same customer.

```sql
CREATE OR REPLACE TEMP VIEW raw_users AS
SELECT * FROM VALUES
    (' Alice@Acme.IO ', 'Alice'),
    ('alice@acme.io',   'Alice A'),   -- same person after normalization
    ('bob@acme.io',     'Bob')
AS t (email_raw, name);

-- Normalise the natural key, then check for collisions
SELECT
    LOWER(TRIM(email_raw)) AS email_key,
    COUNT(*)               AS cnt
FROM raw_users
GROUP BY LOWER(TRIM(email_raw))
HAVING COUNT(*) > 1;
-- email_key     | cnt
-- alice@acme.io | 2      ← two raw rows collapse to one business key
```

!!! warning "Normalise before you trust a natural key"
    Without `LOWER(TRIM(...))`, `' Alice@Acme.IO '` and `'alice@acme.io'` look distinct and
    both load — a silent duplicate. Always canonicalise (case, whitespace, formatting)
    *before* dedup, join, or upsert on a natural key.

---

## :material-swap-horizontal: Natural Key vs Surrogate Key

| Aspect | Natural Key | Surrogate Key |
|--------|-------------|---------------|
| Business meaning | Carries meaning (SKU, email) | Meaningless integer or hash |
| Stability | Can change (email, name) | Never changes |
| Use in MERGE `ON` | Yes — match incoming records | Only when SK is known in source |
| Use in fact table FK | Avoid (changes break history) | Preferred — stable reference |
| Normalisation needed | Yes (TRIM, UPPER, etc.) | No |
| Readable in reports | Yes | No — requires join to dim table |

---

## :material-lightbulb-outline: When to Use Natural Keys

| Scenario | Recommendation |
|----------|---------------|
| Upsert matching in MERGE | Use natural key in `ON` clause |
| SCD change detection | Hash the natural key columns; compare hashes |
| Lookup/search by business value | Index / Z-Order on natural key column |
| Join key in fact tables | Use surrogate key — more stable |
| Ingest from external system with business ID | Preserve natural key, generate surrogate |

!!! warning "Unstable natural keys"
    Never use an unstable natural key (email, phone, username) as a foreign key in a
    fact table. If the natural key changes, all historical fact rows lose their link to
    the correct dimension version. Always introduce a surrogate key for fact table joins.
