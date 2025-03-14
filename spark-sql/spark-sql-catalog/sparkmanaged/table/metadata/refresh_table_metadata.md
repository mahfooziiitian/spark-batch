# refresh table in spark

  REFRESH table partitioned_flights
  show Partitions partitioned_flights
  MSCK REPAIR TABLE partitioned_flights
  show Partitions partitioned_flights
