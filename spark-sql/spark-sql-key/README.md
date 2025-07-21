# Key check

## Candidate Primary Key

```sql

SELECT COUNT(DISTINCT column_name) = COUNT(*) AS is_candidate_pk
FROM your_table;
```

## Candidate Foreign Key

```sql

SELECT COUNT(*) = (
  SELECT COUNT(*) FROM your_table
  WHERE fk_column IN (SELECT pk_column FROM parent_table)
) AS is_candidate_fk;
```
