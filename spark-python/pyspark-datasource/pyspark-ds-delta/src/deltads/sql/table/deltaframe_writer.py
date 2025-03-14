import pyspark
from delta import DeltaTable, configure_spark_with_delta_pip

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
    DeltaTable.createIfNotExists(spark).tableName("default.people10m").addColumn(
        "id", "INT"
    ).addColumn("firstName", "STRING").addColumn("middleName", "STRING").addColumn(
        "lastName", "STRING", comment="surname"
    ).addColumn(
        "gender", "STRING"
    ).addColumn(
        "birthDate", "TIMESTAMP"
    ).addColumn(
        "ssn", "STRING"
    ).addColumn(
        "salary", "INT"
    ).execute()

    # Create or replace table with a path and add properties
    DeltaTable.createOrReplace(spark).addColumn("id", "INT").addColumn(
        "firstName", "STRING"
    ).addColumn("middleName", "STRING").addColumn(
        "lastName", "STRING", comment="surname"
    ).addColumn(
        "gender", "STRING"
    ).addColumn(
        "birthDate", "TIMESTAMP"
    ).addColumn(
        "ssn", "STRING"
    ).addColumn(
        "salary", "INT"
    ).property(
        "description", "table with people data"
    ).location(
        "/tmp/delta/people10m"
    ).execute()
