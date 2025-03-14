# Spark catalog

The highest level abstraction in Spark SQL is the Catalog.

The Catalog is an abstraction for the
storage of metadata about the data stored in your tables as well as other helpful things like
databases, tables, functions, and views.

The catalog is available in the `org.apache.spark.sql.catalog.Catalog` package and contains a number of helpful functions
for doing things like listing tables, databases, and functions.

## Creating table using spark

    CREATE TABLE flights (
    DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
    USING JSON OPTIONS (path '/data/flight-data/json/2015-summary.json')

The specification of the USING syntax in the previous example is of significant importance.

If  you do not specify the format, Spark will default to a Hive SerDe configuration.

This has performance implications for future readers and writers because Hive SerDes are much slower than Spark's native serialization.

Hive users can also use the STORED AS syntax to specify that this should be a Hive table.
