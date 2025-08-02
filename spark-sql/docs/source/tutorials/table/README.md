# spark managed table

  CREATE TABLE  if not exists flights(
    DEST_COUNTRY_NAME STRING,
    ORIGIN_COUNTRY_NAME STRING,
    count LONG
  )
  USING JSON OPTIONS (path '${dataPath}/Json/Flight/2015-summary.json')
  
  CREATE TABLE if not exists flights_csv (
    DEST_COUNTRY_NAME STRING,
    ORIGIN_COUNTRY_NAME STRING COMMENT "remember, the US will be most prevalent",
    count LONG
  )
  USING csv OPTIONS (header true, path '${dataPath}/Csv/Flight/2015-summary.csv')

  CREATE TABLE if not exists flights_from_select
  USING parquet AS SELECT * FROM flights
