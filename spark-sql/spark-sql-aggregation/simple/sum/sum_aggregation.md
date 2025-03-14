# Sum aggregation

## Create tenmporary view

    CREATE OR REPLACE TEMPORARY VIEW states_population
      USING csv
      OPTIONS (
          path 'file:/mnt/d/Data/FileData/Csv/statesPopulation.csv',
          header 'true',
          inferSchema 'true',
          sep ","
      );

## Sum
  
    select 
      State,
      sum(Population) as Total
    from 
      states_population
    group 
      by State
    limit 5;
