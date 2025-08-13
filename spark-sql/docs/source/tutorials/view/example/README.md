# Examples

## 1. Create or replace view with comments

```sql
CREATE OR REPLACE VIEW experienced_employee
    (ID COMMENT 'Unique identification number', Name)
    COMMENT 'View for experienced employees'
    AS SELECT id, name FROM all_employee
WHERE working_years > 5;
```

## 2. Create a global temporary view if it does not exist

```sql
CREATE GLOBAL TEMPORARY VIEW IF NOT EXISTS subscribed_movies
    AS SELECT mo.member_id, mb.full_name, mo.movie_title
        FROM movies AS mo INNER JOIN members AS mb
        ON mo.member_id = mb.id;
```
