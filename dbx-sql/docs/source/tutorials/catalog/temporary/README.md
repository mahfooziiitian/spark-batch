# Temporary

1. Used for session-specific operations.
2. `Includes`:
    - Temporary views
    - Local temp views
3. `Example`: You can create a temp view in your session with:

```sql
CREATE OR REPLACE TEMP VIEW temp_view AS SELECT * FROM table;
```
