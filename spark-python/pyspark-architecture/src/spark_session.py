import os

from pyspark.sql import SparkSession


def create_default_session() -> SparkSession:
    return (
        SparkSession.builder.appName("architecture-demo")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def demo_singleton() -> None:
    """getOrCreate() always returns the same session per JVM."""
    spark1 = SparkSession.builder.appName("Session#1").master("local[*]").getOrCreate()
    spark2 = SparkSession.builder.appName("Session#2").master("local[*]").getOrCreate()
    print(f"Same session: {spark1 is spark2}")  # True
    spark1.stop()


def demo_new_session(spark: SparkSession) -> None:
    """newSession() creates an isolated SQL namespace sharing the same SparkContext."""
    session_b = spark.newSession()
    print(f"Shared SparkContext: {spark.sparkContext is session_b.sparkContext}")  # True
    print(f"Different sessions:  {spark is not session_b}")  # True


if __name__ == "__main__":
    spark = create_default_session()
    spark.sparkContext.setLogLevel("WARN")
    demo_singleton()
    demo_new_session(spark)
    spark.stop()
