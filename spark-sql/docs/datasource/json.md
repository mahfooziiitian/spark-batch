# :material-code-json: JSON Data Source

Spark SQL reads and writes JSON files via the built-in `json` format.
Each line in a JSON file is treated as a separate record (JSON Lines / NDJSON)
by default — use `multiLine = 'true'` for pretty-printed or array-wrapped JSON.

---

## :material-pin: Options Reference

| Option | Default | Description |
|--------|---------|-------------|
| `path` | — | File or directory path |
| `multiLine` | `false` | Parse records spanning multiple lines |
| `inferSchema` | `true` | Infer column types automatically |
| `primitivesAsString` | `false` | Read all primitives as STRING |
| `allowComments` | `false` | Allow Java/C++ style comments in JSON |
| `allowUnquotedFieldNames` | `false` | Accept unquoted keys |
| `allowSingleQuotes` | `true` | Allow single-quoted strings |
| `allowNumericLeadingZeros` | `false` | Accept `0123` as a number |
| `dateFormat` | `yyyy-MM-dd` | Date parsing pattern |
| `timestampFormat` | `yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]` | Timestamp parsing pattern |
| `mode` | `PERMISSIVE` | Error mode: `PERMISSIVE`, `DROPMALFORMED`, `FAILFAST` |
| `columnNameOfCorruptRecord` | `_corrupt_record` | Column for unparseable rows |
| `compression` | `none` | Write compression: `gzip`, `bzip2`, `lz4`, `snappy` |
| `lineSep` | `\n` | Line separator for writing |

---

## :material-flask-outline: Examples

### Read JSON Lines (NDJSON) — one record per line

```sql
CREATE OR REPLACE TEMP VIEW events
USING json
OPTIONS (
    path = 's3://my-bucket/events/2024/'
);

SELECT event_id, user_id, event_type, event_ts FROM events LIMIT 5;
```

### Read multi-line JSON

```sql
-- Each file contains a single pretty-printed JSON object or array
CREATE OR REPLACE TEMP VIEW config_json
USING json
OPTIONS (
    path      = '/mnt/config/settings.json',
    multiLine = 'true'
);
```

### Explicit schema — skip inference for speed

```sql
CREATE OR REPLACE TEMP VIEW clicks (
    event_id   STRING,
    user_id    BIGINT,
    page_url   STRING,
    clicked_at TIMESTAMP,
    metadata   MAP<STRING, STRING>
)
USING json
OPTIONS (
    path            = '/mnt/data/clicks/',
    timestampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
);
```

### Access nested fields

```sql
-- JSON: {"order": {"id": 1, "items": [{"sku": "A", "qty": 2}]}}
CREATE OR REPLACE TEMP VIEW orders_json
USING json
OPTIONS (path = '/mnt/data/orders/', multiLine = 'true');

SELECT
    order.id                     AS order_id,
    order.items[0].sku           AS first_sku,
    SIZE(order.items)            AS item_count
FROM orders_json;
```

### Flatten nested JSON with inline schema

```sql
CREATE OR REPLACE TEMP VIEW user_events (
    event_id   STRING,
    user_id    STRING,
    properties STRUCT<
        browser: STRING,
        os:      STRING,
        ip:      STRING
    >,
    occurred_at TIMESTAMP
)
USING json
OPTIONS (path = '/mnt/data/user_events/');

SELECT
    event_id,
    user_id,
    properties.browser,
    properties.os
FROM user_events;
```

### Explode a JSON array column

```sql
-- JSON: {"order_id": 1, "tags": ["promo", "gift"]}
SELECT
    order_id,
    explode(tags) AS tag
FROM orders_json;
```

### Handle corrupt records

```sql
CREATE OR REPLACE TEMP VIEW safe_json (
    id     INT,
    name   STRING,
    _corrupt_record STRING
)
USING json
OPTIONS (
    path  = '/mnt/data/messy.json',
    mode  = 'PERMISSIVE'
);

SELECT _corrupt_record FROM safe_json WHERE _corrupt_record IS NOT NULL;
```

### Write JSON

```sql
CREATE TABLE exports.order_events
USING json
OPTIONS (compression = 'gzip')
AS
SELECT order_id, customer_id, status, updated_at
FROM orders
WHERE updated_at >= current_date() - INTERVAL 1 DAY;
```

### Convert JSON to Delta (recommended pattern)

```sql
-- Read raw JSON
CREATE OR REPLACE TEMP VIEW raw_events
USING json
OPTIONS (
    path      = 's3://landing/events/',
    multiLine = 'false'
);

-- Write to Delta with partitioning
CREATE TABLE IF NOT EXISTS analytics.events
USING delta
PARTITIONED BY (event_date)
AS
SELECT
    event_id,
    user_id,
    event_type,
    CAST(event_ts AS TIMESTAMP) AS event_ts,
    CAST(event_ts AS DATE)      AS event_date
FROM raw_events;
```

---

## :material-alert-circle: Common Mistakes

| Mistake | Problem | Fix |
|---------|---------|-----|
| Forgetting `multiLine = 'true'` on pretty JSON | Parse error — each `{` treated as a record | Set `multiLine = 'true'` |
| Schema inference on large datasets | Slow full scan | Provide explicit schema |
| Deep nesting without schema | All nested fields as STRING | Declare `STRUCT` types explicitly |
| Reading mixed-schema JSON | Columns become NULL for missing keys | Use `COALESCE` or `GET_JSON_OBJECT` for optional fields |
| `timestampFormat` mismatch | Timestamps parsed as NULL | Match pattern to the actual ISO format in the data |

---

## :material-brain: When to Use JSON

| Scenario | Recommendation |
|----------|----------------|
| Event streams with nested structure | JSON for landing; convert to Delta |
| API responses / webhooks | JSON source |
| Config files (`multiLine`) | JSON with `multiLine = 'true'` |
| Production analytics | Convert to Parquet/Delta first |
| Schema-on-read exploration | JSON with `inferSchema = 'true'` |
