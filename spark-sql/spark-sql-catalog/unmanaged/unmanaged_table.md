# un-managed table

When you define a table from files on disk, you are defining an unmanaged table.

        |CREATE EXTERNAL TABLE hive_flights (
        |DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        |LOCATION '${dataPath}/Csv/FlightSummary/'
        

        |CREATE EXTERNAL TABLE hive_flights_2
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        |LOCATION '${dataPath}/Csv/FlightSummary_ext/'
        |AS SELECT * FROM flights
      

    select * from hive_flights
  select * from hive_flights_2
