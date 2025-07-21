# External tables

export DATA_HOME=C:\Users\Mohammed_Alam\learning\data
set derby.system.home=C:\Users\Mohammed_Alam\learning\data\derby-db\hive_metastore
set HIVE_EXTERNAL = C:\Users\Mohammed_Alam\learning\data\spark\spark-hive-external

```sql

CREATE EXTERNAL TABLE  if not exists hive_bigints(id bigint) 
STORED AS PARQUET LOCATION
```
