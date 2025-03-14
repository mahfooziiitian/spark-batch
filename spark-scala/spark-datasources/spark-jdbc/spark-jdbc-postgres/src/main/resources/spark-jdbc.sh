#Running spark shell with postgres
./bin/spark-shell \
  --driver-class-path postgresql-9.4.1207.jar \
  --jars postgresql-9.4.1207.jar

#Import a CSV file into a table using COPY statement
psql \
  -d postgres \
  -a \
  -f data.sql

