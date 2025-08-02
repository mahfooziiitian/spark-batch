# Bucket

```sql

CREATE TABLE partition_db.student (id INT, name STRING, age INT)
USING CSV
PARTITIONED BY (age)
CLUSTERED BY (Id) INTO 4 buckets;
```
