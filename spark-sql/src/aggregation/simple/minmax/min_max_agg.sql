-- creating table
CREATE OR REPLACE TEMPORARY VIEW flight_summary USING CSV OPTIONS (
    path 'file:/mnt/d/Data/FileData/Csv/Flights/flight-summary.csv',
    header 'true',
    inferschema 'true'
);
-- Multiple aggregation per group
SELECT
    min(count) AS min_count,
    max(count) AS max_count,
    count(count) AS count
FROM flight_summary
GROUP BY origin_airport
