# 🔁 Offset Page Token Pagination in Python (FastAPI + SQLModel)

Offset page token pagination is a hybrid between offset and cursor pagination. Instead of exposing numeric page numbers (page=2), you use a token (like a base64-encoded offset or ID), which:

Makes URLs more opaque and stable

Prevents clients from guessing sensitive details (like total rows)

Allows stateless navigation (token holds offset or marker)

## ✅ Use Case Example

### 🔸 Request

```bash
GET /items?limit=10&page_token=MjA=
```

### 🔸 Meaning

The page_token is base64 for "20", meaning offset = 20.
