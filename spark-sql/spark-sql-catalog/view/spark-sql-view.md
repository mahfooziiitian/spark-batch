# View

Views are based on the result-set of an SQL query. CREATE VIEW constructs a virtual table that has no physical data therefore other operations like ALTER VIEW and DROP VIEW only change metadata.

## Syntax

    CREATE [OR REPLACE] [[GLOBAL] TEMPORARY] VIEW [IF NOT EXISTS] [db_name.]view_name
        create_view_clauses
        AS query;

### OR REPLACE

If a view of same name already exists, it will be replaced.

### [GLOBAL] TEMPORARY

TEMPORARY views are session-scoped and will be dropped when session ends because it skips persisting the definition in the underlying metastore, if any. GLOBAL TEMPORARY views are tied to a system preserved temporary database `global_temp`.

### IF NOT EXISTS

Creates a view if it does not exists.

### create_view_clauses

These clauses are optional and order insensitive. It can be of following formats.

    [(column_name [COMMENT column_comment], ...) ] to specify column-level comments.
    [COMMENT view_comment] to specify view-level comments.
    [TBLPROPERTIES (property_name = property_value, ...)] to add metadata key-value pairs.

### query

A SELECT statement that constructs the view from base tables or other views.

## examples

### Create or replace view for `experienced_employee` with comments

    CREATE OR REPLACE VIEW experienced_employee
        (ID COMMENT 'Unique identification number', Name)
        COMMENT 'View for experienced employees'
        AS SELECT id, name FROM all_employee
            WHERE working_years > 5;

### Create a global temporary view `subscribed_movies` if it does not exist

    CREATE GLOBAL TEMPORARY VIEW IF NOT EXISTS subscribed_movies
        AS SELECT mo.member_id, mb.full_name, mo.movie_title
            FROM movies AS mo INNER JOIN members AS mb
            ON mo.member_id = mb.id;
