SELECT
    avg(count) AS avg_fun,
    sum(count) / count(count) AS avg
FROM flight_summary;
