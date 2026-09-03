-- ============================================================
-- Topic: VARIANT — semi-structured (JSON-like) data in Spark
-- Dialect: Databricks / Spark SQL 4.0+
-- Description: VARIANT stores schema-flexible JSON documents in
--              an optimized binary format, queried with the `:`
--              extraction and `::` cast operators. Covers parsing,
--              field/array/nested access, bracket notation for odd
--              keys, runtime typing, NULL semantics, exploding
--              arrays, constructing VARIANT, a persisted VARIANT
--              column, and a real-world event-analytics demo over
--              a mixed-schema payload.
-- ============================================================

-- --- Setup: raw event stream with heterogeneous payloads -----
-- Each row is a different event shape -- exactly the case where a
-- fixed STRUCT schema would be painful but VARIANT is effortless.
CREATE OR REPLACE TEMP VIEW events AS
SELECT
    event_id,
    PARSE_JSON(raw) AS payload
FROM
    VALUES
    (1, '{"type": "click", "user": {"id": 42, "tier": "gold"}, "pos": [100, 200]}'),
    (2, '{"type": "purchase", "user": {"id": 7, "tier": "silver"}, "amount": 89.90, "items": ["sku-1", "sku-2"]}'),
    (3, '{"type": "signup", "user": {"id": 99}, "referrer": "email", "promo.code": "WELCOME10"}'),
    (4, '{"type": "purchase", "user": {"id": 42, "tier": "gold"}, "amount": 12.50, "items": ["sku-9"]}'),
    (5, '{"type": "click", "user": {"id": 7, "tier": "silver"}, "pos": [10, 15]}')
        AS t (event_id, raw);

-- ============================================================
-- 1. Basic field extraction -- the `:` operator returns VARIANT
--    Add `::<type>` to materialize a concrete SQL type.
-- ============================================================
SELECT
    event_id,
    payload:type::STRING AS event_type,
    payload:user.id::BIGINT AS user_id
FROM events
ORDER BY event_id ASC;
-- event_id | event_type | user_id
-- 1         | click       | 42
-- 2         | purchase    | 7
-- 3         | signup      | 99
-- 4         | purchase    | 42
-- 5         | click       | 7

-- ============================================================
-- 2. Nested paths and array indexing
--    Missing fields yield SQL NULL instead of an error, so one
--    query spans rows of different shapes.
-- ============================================================
SELECT
    event_id,
    payload:user.tier::STRING AS tier,
    payload:pos[0]::INT AS pos_x,
    payload:pos[1]::INT AS pos_y,
    payload:items[0]::STRING AS first_item
FROM events
ORDER BY event_id ASC;

-- ============================================================
-- 3. Bracket notation for keys with dots or special characters
--    `promo.code` cannot use dot access -- it would parse as a
--    nested path. Brackets treat it as a single literal key.
-- ============================================================
SELECT
    event_id,
    payload:['promo.code']::STRING AS promo_code
FROM events
WHERE payload:['promo.code'] IS NOT NULL;
-- event_id | promo_code
-- 3         | WELCOME10

-- ============================================================
-- 4. Runtime typing -- TYPEOF and SCHEMA_OF_VARIANT
--    VARIANT carries its own type tags; inspect them without a
--    declared schema.
-- ============================================================
SELECT
    event_id,
    TYPEOF(payload:type) AS type_of_type,
    TYPEOF(payload:pos) AS type_of_pos,
    SCHEMA_OF_VARIANT(payload) AS inferred_schema
FROM events
ORDER BY event_id ASC;

-- ============================================================
-- 5. Safe extraction with VARIANT_GET / TRY_VARIANT_GET
--    variant_get(v, path, type) is the functional form of `:`.
--    try_variant_get returns NULL on a cast failure instead of
--    raising -- valuable under spark.sql.ansi.enabled = true.
-- ============================================================
SELECT
    event_id,
    VARIANT_GET(payload, '$.user.id', 'BIGINT') AS user_id,
    TRY_VARIANT_GET(payload, '$.amount', 'DECIMAL(10,2)') AS amount
FROM events
ORDER BY event_id ASC;

-- ============================================================
-- 6. NULL semantics -- absent field vs JSON null vs SQL NULL
--    A missing path returns SQL NULL; IS_VARIANT_NULL detects a
--    literal JSON `null`. COALESCE supplies defaults downstream.
-- ============================================================
SELECT
    event_id,
    payload:referrer::STRING AS referrer,
    (payload:referrer IS NULL) AS referrer_missing,
    COALESCE(payload:referrer::STRING, 'organic') AS referrer_filled
FROM events
ORDER BY event_id ASC;

-- ============================================================
-- 7. Exploding a VARIANT array into rows
--    Cast the VARIANT array to a typed ARRAY, then EXPLODE. Only
--    purchase events carry `items`, so others drop out naturally.
-- ============================================================
SELECT
    event_id,
    item
FROM events
    LATERAL VIEW EXPLODE((payload:items)::ARRAY<STRING>) AS item
ORDER BY event_id ASC, item ASC;
-- event_id | item
-- 2         | sku-1
-- 2         | sku-2
-- 4         | sku-9

-- ============================================================
-- 8. Constructing VARIANT -- TO_VARIANT_OBJECT over a STRUCT
--    Build documents programmatically for output or storage.
-- ============================================================
SELECT
    event_id,
    TO_VARIANT_OBJECT(
        NAMED_STRUCT(
            'id', event_id,
            'type', payload:type::STRING,
            'user_id', payload:user.id::BIGINT
        )
    ) AS summary_doc
FROM events
ORDER BY event_id ASC;

-- ============================================================
-- 9. Persisted table with a VARIANT column
--    VARIANT is a first-class storable type, not just a query
--    artifact. Ingest raw JSON now, decide the schema later.
-- ============================================================
DROP TABLE IF EXISTS raw_events;

CREATE TABLE raw_events (
    event_id BIGINT,
    payload VARIANT
) USING PARQUET;

INSERT INTO raw_events
SELECT
    event_id,
    payload
FROM events;

SELECT
    event_id,
    payload:type::STRING AS event_type
FROM raw_events
ORDER BY event_id ASC;

-- ============================================================
-- 10. Real-world demo: analytics over a mixed-schema payload
--     Revenue and event counts per user tier -- computed directly
--     from VARIANT with no upfront flattening or schema migration.
-- ============================================================
SELECT
    payload:user.tier::STRING AS tier,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE payload:type::STRING = 'purchase') AS purchases,
    ROUND(SUM(payload:amount::DECIMAL(10, 2)), 2) AS total_revenue
FROM events
WHERE payload:user.tier IS NOT NULL
GROUP BY payload:user.tier::STRING
ORDER BY total_revenue DESC NULLS LAST;
-- tier   | event_count | purchases | total_revenue
-- gold    | 2           | 1         | 12.50
-- silver  | 2           | 1         | 89.90
