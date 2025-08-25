# Csv data source

```sql

CREATE TABLE my_table
USING csv
OPTIONS (
  path '/path/to/file.csv',
  header 'true',
  inferSchema 'true'
);
```
