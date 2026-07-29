"""Example: Generating test data with Faker and processing with PySpark.

Demonstrates the Faker utility modules for generating realistic test data,
then loading it into Spark for analysis.

Run:
    uv run python examples/faker_data_pipeline.py
"""

import os
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pys.utility.generate_csv_faker_data import generate_people_data, save_to_csv


def main() -> None:
    """Generate fake data, load into Spark, and perform analysis."""
    spark = (
        SparkSession.builder.appName("example-faker-pipeline")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Generate fake data using our utility
    print("=== Generating 50 fake records ===")
    pdf = generate_people_data(count=50, seed=42)
    print(f"Generated {len(pdf)} records")
    print(pdf.head())
    print()

    # Save to CSV and read with Spark
    with tempfile.TemporaryDirectory() as tmp_dir:
        csv_path = Path(tmp_dir) / "people.csv"
        save_to_csv(pdf, csv_path)

        # Load into Spark
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(str(csv_path))
        )

        print("=== Spark DataFrame ===")
        df.show(5)
        df.printSchema()

        # Analysis: age distribution
        print("=== Age Distribution ===")
        df.select(
            F.count("*").alias("total"),
            F.round(F.avg("age"), 1).alias("avg_age"),
            F.min("age").alias("min_age"),
            F.max("age").alias("max_age"),
            F.stddev("age").cast("int").alias("stddev_age"),
        ).show()

        # Top countries
        print("=== Top 5 Countries ===")
        df.groupBy("country").count().orderBy(F.desc("count")).limit(5).show()

        # Salary buckets
        print("=== Salary Brackets ===")
        df.withColumn(
            "bracket",
            F.when(F.col("salary") < 50000, "< 50k")
            .when(F.col("salary") < 80000, "50k-80k")
            .when(F.col("salary") < 100000, "80k-100k")
            .otherwise(">= 100k"),
        ).groupBy("bracket").agg(
            F.count("*").alias("count"), F.round(F.avg("salary"), 0).alias("avg_salary")
        ).orderBy("bracket").show()

    spark.stop()


if __name__ == "__main__":
    main()
