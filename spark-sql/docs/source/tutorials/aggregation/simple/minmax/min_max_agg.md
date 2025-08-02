# creating table

  CREATE OR REPLACE TEMPORARY VIEW flight_summary
  USING csv
  OPTIONS (
          path 'file:/mnt/d/Data/FileData/Csv/Flights/flight-summary.csv',
          header 'true',
          inferSchema 'true'
  );

## multiple aggregation per group

  select
    min(count) as min_count,
    max(count) as max_count,
    count(count) as count
  from
    flight_summary
  group by
    origin_airport
