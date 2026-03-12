from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str, scala_version: str = "2.12", spark_xml_version: str = "0.17.0",
    **kwargs: dict[str, str]
) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            f"com.databricks:spark-xml_{scala_version}:{spark_xml_version}",
        )
        # .configs(kwargs)
        .getOrCreate()
    )
