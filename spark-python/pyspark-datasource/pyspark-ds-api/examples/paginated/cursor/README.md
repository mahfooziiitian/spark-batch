# 🔁 Cursor-Based Pagination

A paginated cursor is a powerful technique used to efficiently paginate through large datasets in APIs or databases.

Unlike offset-based pagination (e.g., LIMIT/OFFSET), cursor-based pagination uses a pointer (the cursor) to mark the position in the dataset.

This approach is faster, more reliable, and consistent, especially in systems with frequently changing data.

## ✅ Key Concepts

| Concept                | Description                                                                    |
| ---------------------- | ------------------------------------------------------------------------------ |
| Cursor                 | A token (e.g., base64 string or unique field) representing the last item seen. |
| After/Before           | Parameters to fetch items after or before a given cursor.                      |
| First/Last             | Number of records to fetch after or before the cursor.                         |
| Deterministic Ordering | Required (usually by unique ID or timestamp) for consistent pagination.        |

## Basic Cursor Pagination Flow

### Request 1 (no cursor)

```http
GET /users?limit=3
```

```json
{
  "data": [
    { "id": 1, "created_at": "2024-01-01T00:00:00Z" },
    { "id": 2, "created_at": "2024-01-02T00:00:00Z" },
    { "id": 3, "created_at": "2024-01-03T00:00:00Z" }
  ],
  "next_cursor": "2024-01-03T00:00:00Z"
}
```

### Request 2 (with cursor)

```http
GET /users?limit=3&after=2024-01-03T00:00:00Z
```

## Comparison with Offset Pagination

| Feature                         | Offset                      | Cursor            |
| ------------------------------- | --------------------------- | ----------------- |
| Performance on large datasets   | ❌ Poor                     | ✅ Good           |
| Consistency (when data changes) | ❌ Risky (skips/duplicates) | ✅ Stable         |
| Supports reverse pagination     | ✅ (harder with cursor)     | ✅ (needs before) |
| Caching                         | ✅ Easier                   | ❌ Harder         |

## SQL

```sql
SELECT * FROM users
WHERE created_at > :cursor
ORDER BY created_at
LIMIT :limit;
```

## Cursor Encoding (Best Practice)

```python
import base64
import json

def encode_cursor(obj):
    return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode()

def decode_cursor(cursor_str):
    return json.loads(base64.urlsafe_b64decode(cursor_str.encode()).decode())
```

## Advanced Use Cases

1. Composite Cursors (e.g., created_at + id) for uniqueness.
2. Bidirectional Pagination with both before and after.
3. Stable Sorting to prevent duplicate/missing records.
4. Relay-style Pagination (edges, node, pageInfo) used in GraphQL APIs.

## 📊 Cursor Pagination: When to Use

| Scenario                     | Cursor | Offset |
| ---------------------------- | ------ | ------ |
| Large dataset                | ✅     | ❌     |
| Frequently changing data     | ✅     | ❌     |
| Infinite scroll (web/mobile) | ✅     | ✅     |
| Exporting static data        | ❌     | ✅     |
