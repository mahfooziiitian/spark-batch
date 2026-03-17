# regexp_replace

`regexp_replace` replaces all substrings matching a regular expression pattern with a replacement string.

## 📌 Syntax

```sql
regexp_replace(str, regexp, rep[, position])
```

- `str`: Input string
- `regexp`: Java-style regular expression pattern
- `rep`: Replacement string (supports backreferences `$1`, `$2`, etc.)
- `position` (optional): Starting position for replacement (1-based)
- Returns: `STRING`

## 🔍 Behavior

1. Replaces **all** occurrences of the pattern (not just the first).
2. Supports backreferences in the replacement string (`$1` for first capturing group).
3. If `position` is specified, characters before that position are left unchanged.

## 🧪 Practical Examples

### Basic Replacement

```sql
SELECT regexp_replace('100-200', '(\\d+)', 'num');
-- Result: 'num-num'
```

### Mask Sensitive Data

```sql
SELECT regexp_replace('SSN: 123-45-6789', '\\d', 'X');
-- Result: 'SSN: XXX-XX-XXXX'
```

### Backreferences — Reformat Dates

```sql
SELECT regexp_replace('2024-01-15', '(\\d{4})-(\\d{2})-(\\d{2})', '$2/$3/$1');
-- Result: '01/15/2024'
```

### Remove Non-Alphanumeric Characters

```sql
SELECT regexp_replace('Hello, World! 123', '[^a-zA-Z0-9 ]', '');
-- Result: 'Hello World 123'
```

### Collapse Whitespace

```sql
SELECT regexp_replace('too    many   spaces', '\\s+', ' ');
-- Result: 'too many spaces'
```

## 🧠 regexp_replace vs translate

| Function | Pattern Type | Scope |
|----------|-------------|-------|
| `regexp_replace` | Regex patterns | Complex pattern replacement |
| `translate(str, from, to)` | Character-by-character | Simple char substitution |
