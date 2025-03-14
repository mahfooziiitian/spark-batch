# pivoting select case

    CREATE OR REPLACE TEMPORARY VIEW flight_2015_summary
    USING csv
    OPTIONS (
            path 'file:/mnt/d/Data/FileData/Csv/Flights/2015-summary.csv',
            header 'true',
            inferSchema 'true'
    );

## Pivoting using select case

    SELECT
         CASE  WHEN DEST_COUNTRY_NAME = 'UNITED STATES'
                 THEN 1
               WHEN DEST_COUNTRY_NAME = 'Egypt'
                 THEN 0
               ELSE -1
         END
    FROM 
      flight_2015_summary
    WHERE
      DEST_COUNTRY_NAME = 'Egypt';
