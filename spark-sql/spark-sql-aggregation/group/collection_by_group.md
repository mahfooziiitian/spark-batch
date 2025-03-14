# Csv

## Create temporary view

    CREATE OR REPLACE TEMPORARY VIEW person_view
    USING csv
    OPTIONS (
        path 'file:/mnt/d/Data/FileData/Csv/person.csv',
        header 'true',
        inferSchema 'true'
    );
