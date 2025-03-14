import pyspark
from delta import configure_spark_with_delta_pip

if __name__ == "__main__":
    builder = (
        pyspark.sql.SparkSession.builder.appName("delta_app")
        # .config('spark.jars.packages', 'io.delta:delta-core_2.13:2.3.0')
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sql(
        """
                CREATE TABLE IF NOT EXISTS default.people10m (
                  id INT,
                  firstName STRING,
                  middleName STRING,
                  lastName STRING,
                  gender STRING,
                  birthDate TIMESTAMP,
                  ssn STRING,
                  salary INT
                ) USING DELTA"""
    )

    # Create or replace table

    spark.sql(
        """
            CREATE OR REPLACE TABLE default.people10m (
                  id INT,
                  firstName STRING,
                  middleName STRING,
                  lastName STRING,
                  gender STRING,
                  birthDate TIMESTAMP,
                  ssn STRING,
                  salary INT
                ) USING DELTA"""
    )

    # creating a table at a path, without creating an entry in the Hive metastore.
    spark.sql(
        """
    CREATE OR REPLACE TABLE delta.`/tmp/delta/people10m` (
      id INT,
      firstName STRING,
      middleName STRING,
      lastName STRING,
      gender STRING,
      birthDate TIMESTAMP,
      ssn STRING,
      salary INT
    ) USING DELTA
    """
    )

    spark.sql(
        """
    CREATE TABLE default.people10m (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
)
USING DELTA
PARTITIONED BY (gender)
"""
    )
