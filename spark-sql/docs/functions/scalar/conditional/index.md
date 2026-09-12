# :material-source-branch: Conditional & NULL-Handling Functions

Functions for branching logic, boolean predicates, and safe `NULL` handling —
the building blocks of row-level decision-making in SQL.

______________________________________________________________________

## :material-book-open-variant: In This Section

| Page                      | Contents                                                                |
| ------------------------- | ----------------------------------------------------------------------- |
| [NULL](null.md)           | `COALESCE`, `NVL`, `NVL2`, `NULLIF`, `IFNULL`, `<=>` null-safe equality |
| [Predicate](predicate.md) | `ISNULL`, `ISNAN`, `IN`, `BETWEEN`, three-valued logic                  |
| [Control](control.md)     | `IF`, `IIF`, `CASE WHEN`, `DECODE`                                      |

> **Rule of thumb:** `NULL` functions answer "what value should I use instead
> of missing data?"; `Predicate` functions answer "is this condition true,
> false, or unknown?"; `Control` functions answer "which value should this
> expression evaluate to, given a condition?"
