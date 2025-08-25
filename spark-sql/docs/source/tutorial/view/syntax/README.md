# Syntax

```sql
CREATE 
    [OR REPLACE] 
    [[GLOBAL] TEMPORARY] VIEW 
    [IF NOT EXISTS] 
    [db_name.]view_name
    create_view_clauses
AS query;
```

Keyword|Description
---|---
`CREATE OR REPLACE`|If a view of same name already exists, it will be replaced.
`[GLOBAL] TEMPORARY`|`TEMPORARY` views are session-scoped and will be dropped when session ends because it skips persisting the definition in the underlying metastore, if any. `GLOBAL TEMPORARY` views are tied to a system preserved temporary database `global_temp`.
`IF NOT EXISTS`|Creates a view if it does not exists.
`create_view_clauses`|These clauses are optional and order insensitive. It can be of following formats.
`query`|A SELECT statement that constructs the view from base tables or other views.

```sql
[(column_name [COMMENT column_comment], ...) ] to specify column-level comments.
[COMMENT view_comment] to specify view-level comments.
[TBLPROPERTIES (property_name = property_value, ...)] to add metadata key-value pairs.
```
