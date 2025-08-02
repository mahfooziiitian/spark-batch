# Insert

## Insert into

```sql
-- Append rows
INSERT INTO TABLE target_table
SELECT * FROM source_table;
```

```sql
INSERT INTO flights_from_select
SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 20
```

## Insert overwrite
