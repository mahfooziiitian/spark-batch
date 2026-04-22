# :material-code-json: JSON Data Source

Spark SQL can read and write JSON files using the built-in JSON data source.
It supports schema inference and custom parsing options.

---

## :material-pin: Read JSON

```sql
CREATE OR REPLACE TEMP VIEW events
USING json
OPTIONS (path 's3://data/events/');
```

---

## :material-pin: Write JSON

```sql
CREATE TABLE events_json
USING json
AS SELECT * FROM events;
```

---

## :material-magnify: Common Options

| Option | Description |
|--------|-------------|
| `multiLine` | Allow records across multiple lines |
| `inferSchema` | Infer column types |
| `timestampFormat` | Custom timestamp parsing |
| `dateFormat` | Custom date parsing |

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Semi-structured data | JSON data source |
| Known schema | Provide explicit schema for speed |
| Large datasets | Avoid inference for performance |
