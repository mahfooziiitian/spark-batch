# 📘 Page Number Pagination

Page number pagination is the most familiar and widely used pagination style, especially in user-facing applications (e.g., blogs, e-commerce, admin panels).

It breaks a dataset into pages and lets users request specific page numbers.

## ✅ Core Concepts

| Parameter            | Purpose                                                            |
| -------------------- | ------------------------------------------------------------------ |
| page                 | Specifies the current page number (starting from 1 by convention). |
| page_size (or limit) | How many items per page.                                           |

## 📚 1. Standard Page Number Pagination

### Structure

```http
GET /items?page=3&page_size=10
```

`🔸 Characteristics`

1. Most common.
2. Good for table UIs, dashboards.
3. Easy to implement and understand.

## 🔄 2. Page Number with Total Count

Adds total item count and total pages in the response metadata.

```json
{
  "data": [...],
  "meta": {
    "page": 3,
    "page_size": 10,
    "total": 97,
    "total_pages": 10
  }
}
```

`🧠 Pros`

1. Easy to show "Page 3 of 10".
2. Supports navigation, next/prev.

`❗ Cons`

Requires a full COUNT(\*), which can be slow on large tables.

## 🚫 3. Page Number Without Total Count

Skips total count to improve performance on huge datasets.

`🔸 Request`

```http
GET /items?page=5&page_size=10
```

`🔸 Response`

```json
{
  "data": [...],
  "meta": {
    "page": 5,
    "page_size": 10,
    "has_next": true
  }
}
```

`🧠 Pros`

1. Fast.
2. Suitable for streaming, infinite scroll.

`❗ Cons`

1. Cannot show total pages.
2. No "Go to page 10" button.

## 📉 4. Descending Page Number

Paginate from the latest records (e.g., newest blog posts).

`🔸 Request`

```http
GET /items?page=1&page_size=10&order=desc
```

`🔸 SQL`

```sql
SELECT * FROM items
ORDER BY created_at DESC
LIMIT 10 OFFSET (page - 1) * page_size;
```

`🧠 Pros`

1. Better for feeds, notifications.

`❗ Cons`

Combined with unstable ordering, can lead to missing/duplicated data.

## 🧮 5. Page Number with Display Window (UI-friendly)

Pagination logic includes a "window" of nearby page numbers.

```json
{
  "meta": {
    "current_page": 5,
    "total_pages": 20,
    "pages": [3, 4, 5, 6, 7],
    "has_next": true,
    "has_previous": true
  }
}
```

## 📦 6. Page Number + Offset Hybrid

Allows both page and offset for flexibility.

`🔸 Request`

```http
GET /items?page=2&page_size=10&offset=15
```

If offset is provided, it takes precedence.

Otherwise,

```math
offset = (page - 1) * page_size
```

Use case: APIs that support flexible pagination styles for different clients.

## 🔍 7. Paginated Grouping (Page by Category/Tag)

Pagination is scoped within a group, like tags, categories, or sections.

```http
GET /articles?category=ai&page=2&page_size=5
```

Result: Page 2 of AI articles only.

## 💡 8. Auto Page Navigation (Next/Prev Links)

Follows REST best practices with navigation links.

### Example

```json
{
  "data": [...],
  "meta": {
  "page": 2,
  "page_size": 10,
  "total": 100
},
"links": {
  "self": "/items?page=2",
  "next": "/items?page=3",
  "prev": "/items?page=1",
  "first": "/items?page=1",
  "last": "/items?page=10"
}
}
```
