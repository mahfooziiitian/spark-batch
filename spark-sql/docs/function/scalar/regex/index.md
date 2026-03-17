# Regex Functions Overview

Spark SQL provides a comprehensive set of regular expression functions for pattern matching,
extraction, and replacement within string data.

## 📌 Functions at a Glance

| Function | Description | Returns |
|----------|-------------|---------|
| `REGEXP_LIKE(str, regex)` | Test if string matches pattern | `BOOLEAN` |
| `RLIKE` / `ILIKE` | SQL operator aliases for regex matching | `BOOLEAN` |
| `REGEXP_EXTRACT(str, regex, idx)` | Extract capturing group from match | `STRING` |
| `REGEXP_EXTRACT_ALL(str, regex, idx)` | Extract all occurrences | `ARRAY<STRING>` |
| `REGEXP_REPLACE(str, regex, rep)` | Replace matched substrings | `STRING` |
| `REGEXP_SUBSTR(str, regex)` | Return the matched substring | `STRING` |
| `REGEXP_INSTR(str, regex)` | Return position of first match | `INT` |
| `REGEXP_COUNT(str, regex)` | Count number of matches | `INT` |

## 🔍 Regex Syntax Notes

Spark SQL uses **Java-style** regular expressions. Key patterns:

| Pattern | Meaning |
|---------|---------|
| `.` | Any single character |
| `\d` | Digit `[0-9]` (escape as `\\d` in SQL strings) |
| `\w` | Word character `[a-zA-Z0-9_]` |
| `*`, `+`, `?` | Quantifiers (zero+, one+, optional) |
| `()` | Capturing group |
| `[abc]` | Character class |
| `^` / `$` | Start / end of string |

## 🧪 Quick Examples

```sql
-- Match: does string contain digits?
SELECT REGEXP_LIKE('order-123', '\\d+');  -- true

-- Extract: pull out the number
SELECT REGEXP_EXTRACT('order-123', '(\\d+)', 1);  -- '123'

-- Replace: mask digits
SELECT REGEXP_REPLACE('call 555-1234', '\\d', 'X');  -- 'call XXX-XXXX'

-- Position: where does the match start?
SELECT REGEXP_INSTR('hello world', 'world');  -- 7

-- Count: how many matches?
SELECT REGEXP_COUNT('a1b2c3', '\\d');  -- 3
```
