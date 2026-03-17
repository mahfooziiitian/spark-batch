# Web Functions

Web functions in Spark SQL parse and extract components from URLs.

## 📌 parse_url

```sql
parse_url(url, partToExtract[, key])
```

- `url`: The URL string to parse
- `partToExtract`: One of `HOST`, `PATH`, `QUERY`, `REF`, `PROTOCOL`, `FILE`, `AUTHORITY`, `USERINFO`
- `key` (optional): Extract a specific query parameter by name
- Returns: `STRING`

## 🔍 Behavior

1. Parses the URL according to standard URL structure.
2. Returns the specified component, or NULL if the URL is invalid.
3. When `key` is provided with `QUERY`, extracts that specific parameter's value.

## 🧪 Practical Examples

### Extract Host

```sql
SELECT parse_url('http://spark.apache.org/path?query=1', 'HOST');
-- Result: 'spark.apache.org'
```

### Extract Protocol

```sql
SELECT parse_url('https://docs.example.com/api/v2', 'PROTOCOL');
-- Result: 'https'
```

### Extract Full Query String

```sql
SELECT parse_url('http://spark.apache.org/path?query=1', 'QUERY');
-- Result: 'query=1'
```

### Extract Specific Query Parameter

```sql
SELECT parse_url('http://example.com/search?q=spark&lang=en', 'QUERY', 'q');
-- Result: 'spark'

SELECT parse_url('http://example.com/search?q=spark&lang=en', 'QUERY', 'lang');
-- Result: 'en'
```

### Extract Path

```sql
SELECT parse_url('https://example.com/api/v2/users?limit=10', 'PATH');
-- Result: '/api/v2/users'
```

### Parse Multiple Components

```sql
SELECT
  parse_url(url, 'PROTOCOL') AS protocol,
  parse_url(url, 'HOST')     AS host,
  parse_url(url, 'PATH')     AS path,
  parse_url(url, 'QUERY')    AS query
FROM VALUES ('https://spark.apache.org/docs/latest?format=pdf') AS t(url);
```

## 🧠 URL Components Reference

| Part | Example URL: `https://user:pass@host.com:8080/path?q=1#ref` |
|------|-------------------------------------------------------------|
| `PROTOCOL` | `https` |
| `USERINFO` | `user:pass` |
| `HOST` | `host.com` |
| `AUTHORITY` | `user:pass@host.com:8080` |
| `PATH` | `/path` |
| `QUERY` | `q=1` |
| `REF` | `ref` |
| `FILE` | `/path?q=1` |
