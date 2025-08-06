-- Sum aggregation
-- Create tenmporary view
CREATE OR REPLACE TEMPORARY VIEW states_population USING CSV OPTIONS (
    path 'file:/mnt/d/Data/FileData/Csv/statesPopulation.csv',
    header 'true',
    inferschema 'true',
    sep ','
);
-- Sum
SELECT
    state,
    sum(population) AS total
FROM states_population
GROUP BY state
LIMIT 5;
