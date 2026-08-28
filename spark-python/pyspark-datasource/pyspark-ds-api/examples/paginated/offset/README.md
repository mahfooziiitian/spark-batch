# Offset-Based Pagination

Offset pagination allows you to fetch chunks (pages) of a large dataset by specifying:

1. a limit (how many items per page), and
2. an offset (how many items to skip).

## 🔁 How It Works

### Request 1 (Page 1)

```http
GET /users?limit=10&offset=0
```

### Request 2 (Page 2):

```http
GET /users?limit=10&offset=10
```

## SQL

```sql
SELECT * FROM users
ORDER BY created_at
LIMIT 10 OFFSET 20;
```

## ⚖️ Pros and Cons

| Aspect             | Offset Pagination                                                |
| ------------------ | ---------------------------------------------------------------- |
| ✅ Simplicity      | Very easy to implement and understand                            |
| ✅ Compatibility   | Works with almost all databases and UIs                          |
| ✅ Page Navigation | Easy to jump to any page (page = 5, offset = 40)                 |
| ❌ Performance     | Slower for large offset values — full scan is needed             |
| ❌ Consistency     | Risk of duplicate/missing rows if data changes during pagination |
| ❌ Expensive COUNT | Counting total results can be expensive on large tables          |

## 🧠 Common Use Cases

| Use Case            | Suitable?                   |
| ------------------- | --------------------------- |
| Admin dashboards    | ✅ Yes                      |
| Static reports      | ✅ Yes                      |
| Infinite scrolling  | ❌ Use cursor instead       |
| Large, dynamic data | ❌ Cursor is more efficient |

## Pagination Meta Format (Standard)

```json
{
  "data": [...],
  "meta": {
    "total": 124,
    "limit": 10,
    "offset": 30,
    "next_offset": 40,
    "prev_offset": 20
  }
}
```

## 🔐 Enhancing Performance

1. Add Indexes on the ordering column (e.g., created_at, id).
2. Avoid large offsets if possible. OFFSET 10000 performs poorly.
3. Paginate by WHERE id > last_seen_id if performance becomes an issue → consider cursor pagination.
4. Use LIMIT + 1 trick to check if a next page exists.

## 💡 Offset vs Cursor: Side-by-Side

| Feature                  | Offset                        | Cursor                    |
| ------------------------ | ----------------------------- | ------------------------- |
| Simplicity               | ✅ Easy to implement          | ❌ Slightly complex       |
| Performance (large data) | ❌ Degrades at high offsets   | ✅ Constant time          |
| Jump to arbitrary page   | ✅ Supported                  | ❌ Not easy               |
| Real-time consistency    | ❌ May skip/duplicate records | ✅ Stable                 |
| Backward pagination      | ✅ Easy                       | ⚠️ Needs careful handling |

## 🧪 Testing Strategy

1. Verify total and offset + limit logic.
2. Handle offset out of bounds gracefully (return empty).
3. Prevent negative values for limit and offset.
4. Validate if limit exceeds max allowed (e.g., 1000).
