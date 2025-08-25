# Update

Supported on Delta Lake tables (not standard Parquet/Hive).

```sql
UPDATE target_table
SET column1 = value1
WHERE condition;
```

```sql
UPDATE customers
SET loyalty_points = loyalty_points + 10
WHERE region = 'West';
```
