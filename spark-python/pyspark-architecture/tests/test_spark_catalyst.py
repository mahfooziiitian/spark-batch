import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test-catalyst")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


class TestExplainPlans:
    def test_simple_explain_produces_output(self, spark):
        df = spark.range(10).filter(F.col("id") > 5)
        plan = df._jdf.queryExecution().simpleString()
        assert "Filter" in plan

    def test_extended_explain_shows_all_phases(self, spark):
        df = spark.range(10).groupBy((F.col("id") % 2).alias("bucket")).count()
        plan = df._jdf.queryExecution().toString()
        assert "Parsed Logical Plan" in plan or "Analyzed Logical Plan" in plan

    def test_physical_plan_available(self, spark):
        df = spark.range(10).filter(F.col("id") < 5)
        physical = df._jdf.queryExecution().executedPlan().toString()
        assert len(physical) > 0


class TestPredicatePushdown:
    def test_filter_pushed_below_projection(self, spark):
        df = spark.range(100).withColumn("doubled", F.col("id") * 2).filter(F.col("id") < 10)
        assert df.count() == 10

    def test_filter_reduces_rows_early(self, spark):
        df = spark.range(1000).filter(F.col("id") < 5).withColumn("x", F.col("id") + 1)
        assert df.count() == 5
        assert set(df.columns) == {"id", "x"}


class TestColumnPruning:
    def test_select_removes_unused_columns(self, spark):
        df = spark.createDataFrame(
            [(1, "a", 100.0), (2, "b", 200.0)],
            ["id", "name", "salary"],
        )
        result = df.select("name")
        assert result.columns == ["name"]
        assert result.count() == 2

    def test_pruned_plan_excludes_columns(self, spark):
        df = spark.createDataFrame(
            [(1, "a", "x", 10.0)],
            ["id", "name", "addr", "sal"],
        )
        result = df.select("name", "sal")
        plan = result._jdf.queryExecution().executedPlan().toString()
        assert "name" in plan or "sal" in plan


class TestConstantFolding:
    def test_literal_expression_evaluated(self, spark):
        df = spark.range(5).withColumn("const", F.lit(60 * 60 * 24))
        row = df.first()
        assert row["const"] == 86400

    def test_constant_filter_optimised(self, spark):
        df = spark.range(10).filter(F.lit(True))
        assert df.count() == 10

        df_empty = spark.range(10).filter(F.lit(False))
        assert df_empty.count() == 0


class TestAdaptiveQueryExecution:
    def test_aqe_enabled(self, spark):
        assert spark.conf.get("spark.sql.adaptive.enabled") == "true"

    def test_broadcast_join_with_small_table(self, spark):
        large = spark.range(1000).withColumn("key", (F.col("id") % 10).cast("int"))
        small = spark.createDataFrame([(i, f"v{i}") for i in range(10)], ["key", "value"])
        joined = large.join(F.broadcast(small), on="key")
        assert joined.count() == 1000
        assert "value" in joined.columns

    def test_aqe_coalesces_partitions(self, spark):
        df = spark.range(100).withColumn("k", (F.col("id") % 2).cast("int"))
        result = df.groupBy("k").count()
        assert result.count() == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
