"""
PySpark CI Test Suite
======================
Validates that PySpark is correctly installed and functional in the CI environment.
Covers SparkSession creation, DataFrame operations, SQL, window functions, and
Parquet I/O.

Run with pytest:
    pytest ci/test_pyspark.py -v

Or run directly:
    python ci/test_pyspark.py
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window


# ── Shared SparkSession fixture ───────────────────────────────────────────────
@pytest.fixture(scope="session")
def spark():
    """Single SparkSession reused across all tests in the session."""
    session = (SparkSession.builder
               .appName("ci-pyspark-tests")
               .master("local[2]")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ── SparkSession ──────────────────────────────────────────────────────────────
class TestSparkSession:
    def test_session_created(self, spark):
        assert spark is not None

    def test_spark_version_3_or_higher(self, spark):
        major = int(spark.version.split(".")[0])
        assert major >= 3, f"Expected Spark 3.x, got {spark.version}"

    def test_master_is_local(self, spark):
        assert spark.sparkContext.master.startswith("local")

    def test_app_name_set(self, spark):
        assert spark.sparkContext.appName != ""


# ── DataFrame basics ──────────────────────────────────────────────────────────
class TestDataFrame:
    def test_create_from_list(self, spark):
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        assert df.count() == 2

    def test_explicit_schema(self, spark):
        schema = StructType([
            StructField("id",    IntegerType(), False),
            StructField("name",  StringType(),  True),
            StructField("score", DoubleType(),  True),
        ])
        df = spark.createDataFrame([(1, "Alice", 9.5)], schema)
        assert df.schema == schema

    def test_filter(self, spark):
        df = spark.range(10)
        assert df.filter(F.col("id") > 5).count() == 4

    def test_withcolumn_expression(self, spark):
        df = spark.createDataFrame([(2, 3.0), (4, 5.0)], ["x", "y"])
        result = df.withColumn("z", F.col("x") * F.col("y"))
        rows = {row["x"]: row["z"] for row in result.collect()}
        assert rows[2] == 6.0
        assert rows[4] == 20.0

    def test_groupby_agg(self, spark):
        data = [("A", 10), ("A", 20), ("B", 30), ("B", 40)]
        df = spark.createDataFrame(data, ["grp", "val"])
        result = df.groupBy("grp").agg(F.sum("val").alias("total"))
        totals = {row["grp"]: row["total"] for row in result.collect()}
        assert totals["A"] == 30
        assert totals["B"] == 70

    def test_distinct_count(self, spark):
        data = [("x",), ("x",), ("y",), ("z",)]
        df = spark.createDataFrame(data, ["val"])
        assert df.distinct().count() == 3

    def test_join(self, spark):
        left  = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "l"])
        right = spark.createDataFrame([(1, "x"), (3, "y")], ["id", "r"])
        joined = left.join(right, on="id", how="inner")
        assert joined.count() == 1
        assert joined.collect()[0]["l"] == "a"


# ── SQL interface ─────────────────────────────────────────────────────────────
class TestSQL:
    def test_temp_view_count(self, spark):
        df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        df.createOrReplaceTempView("nums")
        result = spark.sql("SELECT COUNT(*) AS cnt FROM nums")
        assert result.collect()[0]["cnt"] == 3

    def test_sql_aggregation_order(self, spark):
        data = [("North", 100), ("South", 200), ("North", 150)]
        spark.createDataFrame(data, ["region", "sales"]).createOrReplaceTempView("sales_v")
        rows = spark.sql(
            "SELECT region, SUM(sales) AS total FROM sales_v GROUP BY region ORDER BY total DESC"
        ).collect()
        assert rows[0]["region"] == "South"
        assert rows[0]["total"] == 200

    def test_sql_filter_and_alias(self, spark):
        df = spark.range(20)
        df.createOrReplaceTempView("nums20")
        result = spark.sql("SELECT id AS num FROM nums20 WHERE id > 15")
        assert result.count() == 4


# ── Window functions ──────────────────────────────────────────────────────────
class TestWindowFunctions:
    def test_rank(self, spark):
        data = [("A", 10), ("B", 30), ("C", 20)]
        df = spark.createDataFrame(data, ["name", "score"])
        w = Window.orderBy(F.desc("score"))
        ranked = df.withColumn("rnk", F.rank().over(w))
        top = ranked.filter(F.col("rnk") == 1).collect()[0]
        assert top["name"] == "B"

    def test_running_total(self, spark):
        data = [(1, 10), (2, 20), (3, 30)]
        df = spark.createDataFrame(data, ["step", "val"])
        w = Window.orderBy("step").rowsBetween(Window.unboundedPreceding, 0)
        result = df.withColumn("running", F.sum("val").over(w))
        final = result.orderBy(F.desc("step")).first()
        assert final["running"] == 60

    def test_lag(self, spark):
        data = [(1, 100), (2, 200), (3, 300)]
        df = spark.createDataFrame(data, ["day", "sales"])
        w = Window.orderBy("day")
        result = df.withColumn("prev_sales", F.lag("sales", 1).over(w))
        rows = {r["day"]: r["prev_sales"] for r in result.collect()}
        assert rows[1] is None
        assert rows[2] == 100
        assert rows[3] == 200


# ── Parquet I/O ───────────────────────────────────────────────────────────────
class TestParquetIO:
    def test_write_and_read(self, spark, tmp_path):
        path = str(tmp_path / "test.parquet")
        df = spark.createDataFrame([(1, "x"), (2, "y"), (3, "z")], ["id", "val"])
        df.write.mode("overwrite").parquet(path)
        read_back = spark.read.parquet(path)
        assert read_back.count() == 3
        assert set(read_back.columns) == {"id", "val"}

    def test_partitioned_write(self, spark, tmp_path):
        path = str(tmp_path / "partitioned.parquet")
        data = [("A", 1), ("A", 2), ("B", 3)]
        df = spark.createDataFrame(data, ["grp", "val"])
        df.write.mode("overwrite").partitionBy("grp").parquet(path)
        read_back = spark.read.parquet(path)
        assert read_back.count() == 3


# ── Entry point for direct execution ─────────────────────────────────────────
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
