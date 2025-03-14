# Partition managed table

  CREATE TABLE partitioned_flights USING parquet PARTITIONED BY(DEST_COUNTRY_NAME)
  AS SELECT DEST_COUNTRY_NAME
    , ORIGIN_COUNTRY_NAME
    , count FROM flights LIMIT 5

  select * from partitioned_flights
