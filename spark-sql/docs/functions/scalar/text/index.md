# :material-alphabetical-variant: Text Functions

Functions that operate on `STRING` values — from basic manipulation (case, trim,
padding, concatenation) to full regular-expression matching and extraction.

______________________________________________________________________

## :material-book-open-variant: In This Section

| Page                    | Contents                                                              |
| ----------------------- | --------------------------------------------------------------------- |
| [String](string.md)     | `UPPER`, `TRIM`, `SPLIT`, `SUBSTRING`, `CONCAT_WS`, padding, reversal |
| [Regex](regex/index.md) | `REGEXP_LIKE`, `REGEXP_EXTRACT`, `REGEXP_REPLACE`, `RLIKE`, `ILIKE`   |

> **Rule of thumb:** reach for plain string functions when you know the exact
> characters/positions involved; reach for regex when the shape of the text
> varies and you need pattern matching.
