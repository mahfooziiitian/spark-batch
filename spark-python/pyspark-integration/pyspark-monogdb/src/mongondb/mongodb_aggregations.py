import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:mongo@127.0.0.1:27017")
MONGO_DB = os.environ.get("MONGO_DB", "tutorial")


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("mongodb-aggregations")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",
        )
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    sales = spark.createDataFrame(
        [
            ("North", "2024-01", "Electronics", 1200.00),
            ("North", "2024-01", "Clothing", 450.50),
            ("North", "2024-02", "Electronics", 980.00),
            ("North", "2024-02", "Clothing", 520.75),
            ("South", "2024-01", "Electronics", 1500.00),
            ("South", "2024-01", "Clothing", 380.25),
            ("South", "2024-02", "Electronics", 1100.00),
            ("South", "2024-02", "Clothing", 610.50),
            ("East", "2024-01", "Electronics", 890.00),
            ("East", "2024-01", "Clothing", 340.00),
            ("East", "2024-02", "Electronics", 1050.00),
            ("East", "2024-02", "Clothing", 475.25),
        ],
        ["region", "month", "category", "revenue"],
    )

    # Write raw sales to MongoDB
    (sales.write
     .format("mongodb")
     .mode("overwrite")
     .option("database", MONGO_DB)
     .option("collection", "sales")
     .save())

    # Read back from MongoDB
    sales_from_mongo = (
        spark.read
        .format("mongodb")
        .option("database", MONGO_DB)
        .option("collection", "sales")
        .load()
    )

    # Aggregate revenue by region
    region_summary = (
        sales_from_mongo
        .groupBy("region")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.round(F.avg("revenue"), 2).alias("avg_revenue"),
            F.countDistinct("category").alias("categories"),
            F.count("*").alias("transaction_count"),
        )
        .orderBy(F.desc("total_revenue"))
    )

    print("Revenue by region:")
    region_summary.show()

    (region_summary.write
     .format("mongodb")
     .mode("overwrite")
     .option("database", MONGO_DB)
     .option("collection", "region_summary")
     .save())

    # Running total per region using a window function
    w = (
        Window
        .partitionBy("region")
        .orderBy("month")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    monthly_running = (
        sales_from_mongo
        .groupBy("region", "month")
        .agg(F.round(F.sum("revenue"), 2).alias("monthly_revenue"))
        .withColumn("running_total", F.round(F.sum("monthly_revenue").over(w), 2))
        .orderBy("region", "month")
    )

    print("Monthly revenue with running total:")
    monthly_running.show()

    (monthly_running.write
     .format("mongodb")
     .mode("overwrite")
     .option("database", MONGO_DB)
     .option("collection", "monthly_running_totals")
     .save())

    # Rank regions by total revenue using a window function
    rank_window = Window.orderBy(F.desc("total_revenue"))

    ranked_regions = (
        region_summary
        .withColumn("rank", F.dense_rank().over(rank_window))
        .select("rank", "region", "total_revenue", "avg_revenue")
    )

    print("Region rankings by revenue:")
    ranked_regions.show()

    (ranked_regions.write
     .format("mongodb")
     .mode("overwrite")
     .option("database", MONGO_DB)
     .option("collection", "region_rankings")
     .save())

    # spark.stop()


if __name__ == "__main__":
    main()
