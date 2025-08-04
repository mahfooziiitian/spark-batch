# Tables

Describes tables and views defined within the catalog.

`INFORMATION_SCHEMA.TABLES` contains the object-level metadata for tables and views (relations) within the `local catalog`, or `all catalogs visible to the workspace`, if owned by the `SYSTEM` catalog.

The `rows returned` are `limited` to the `relations` the user is `privileged` to `interact with`.

## Metadata

Metadata table|Description
---|---
TABLE_CONSTRAINTS|Describes metadata for all primary and foreign key constraints within the catalog.
TABLE_PRIVILEGES|Lists principals that have privileges on the tables and views in the catalog.
TABLE_SHARE_USAGE|Describes the tables referenced in shares.
TABLE_TAGS|Contains table tagging metadata within a table.
TABLES|Describes tables and views defined within the catalog.
VIEWS|Describes view specific information about the views in the catalog.

## Queries

### List All Tables in a Catalog

#### Using local catalog

This query lists all tables in the `main` catalog, including their schema and type.

```sql
SELECT table_catalog,
       table_schema,
       table_name,
       table_type
FROM main.information_schema.tables;
```

#### Using system catalog

```sql
SELECT table_catalog,
       table_schema,
       table_name,
       table_type
FROM system.information_schema.tables
where table_catalog = 'main';
```

### List All Tables in a Schema

#### Using local catalog schema

```sql
SELECT 
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM main.information_schema.tables
WHERE table_schema = 'schema_name';
```

#### Using system catalog schema

```sql
SELECT 
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM system.information_schema.tables
WHERE table_schema = 'schema_name';
```
