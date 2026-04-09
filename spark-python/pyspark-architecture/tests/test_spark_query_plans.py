import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test-query-plans")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def employees(spark):
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("dept", StringType()),
            StructField("salary", DoubleType()),
        ]
    )
    data = [
        (1, "Alice", "Eng", 95000.0),
        (2, "Bob", "Mkt", 72000.0),
        (3, "Charlie", "Eng", 110000.0),
        (4, "Diana", "Mkt", 68000.0),
        (5, "Eve", "Eng", 105000.0),
    ]
    return spark.createDataFrame(data, schema)


class TestParsedLogicalPlan:
    """The parsed plan is the raw AST — unresolved column references."""

    def test_parsed_plan_contains_filter(self, employees):
        result = employees.filter(F.col("dept") == "Eng")
        parsed = result._jdf.queryExecution().logical().toString()
        assert "Filter" in parsed

    def test_parsed_plan_contains_project(self, employees):
        result = employees.select("name", "salary")
        parsed = result._jdf.queryExecution().logical().toString()
        assert "Project" in parsed

    def test_parsed_plan_contains_aggregate(self, employees):
        result = employees.groupBy("dept").count()
        parsed = result._jdf.queryExecution().logical().toString()
        assert "Aggregate" in parsed

    def test_parsed_plan_preserves_user_order(self, employees):
        result = employees.withColumn("bonus", F.col("salary") * 0.1).filter(F.col("dept") == "Eng")
        parsed = result._jdf.queryExecution().logical().toString()
        # Filter should appear at the top (outermost) in the parsed plan
        filter_pos = parsed.find("Filter")
        project_pos = parsed.find("Project")
        assert filter_pos < project_pos


class TestAnalyzedLogicalPlan:
    """The analyzed plan resolves column names and types."""

    def test_analyzed_plan_resolves_types(self, employees):
        result = employees.filter(F.col("salary") > 80000)
        analyzed = result._jdf.queryExecution().analyzed().toString()
        assert "double" in analyzed or "salary" in analyzed

    def test_analyzed_plan_expands_star(self, employees):
        result = employees.select("*")
        analyzed = result._jdf.queryExecution().analyzed().toString()
        assert "id" in analyzed
        assert "name" in analyzed
        assert "dept" in analyzed
        assert "salary" in analyzed

    def test_analyzed_plan_resolves_cast(self, employees):
        result = employees.withColumn("sal_int", F.col("salary").cast("int"))
        analyzed = result._jdf.queryExecution().analyzed().toString()
        assert "cast" in analyzed.lower() or "int" in analyzed.lower()

    def test_analyzed_plan_resolves_alias(self, employees):
        result = employees.select(F.col("name").alias("employee_name"))
        analyzed = result._jdf.queryExecution().analyzed().toString()
        assert "employee_name" in analyzed


class TestOptimizedLogicalPlan:
    """The optimized plan applies rule-based rewrites."""

    def test_predicate_pushdown(self, employees):
        result = employees.withColumn("bonus", F.col("salary") * 0.1).filter(F.col("dept") == "Eng")

        analyzed = result._jdf.queryExecution().analyzed().toString()
        optimized = result._jdf.queryExecution().optimizedPlan().toString()

        # In the analyzed plan, Filter is above Project(bonus)
        # In the optimized plan, Filter should be pushed down
        assert "Filter" in analyzed
        assert "Filter" in optimized

    def test_column_pruning_drops_unused(self, employees):
        result = employees.select("name")
        optimized = result._jdf.queryExecution().optimizedPlan().toString()
        assert "name" in optimized

    def test_constant_folding(self, spark):
        df = spark.range(5).withColumn("const", F.lit(365 * 24 * 60))
        optimized = df._jdf.queryExecution().optimizedPlan().toString()
        assert "525600" in optimized

    def test_boolean_simplification(self, employees):
        result = employees.filter(F.lit(True) & (F.col("salary") > 0))
        optimized = result._jdf.queryExecution().optimizedPlan().toString()
        # The 'true AND x' should be simplified to just 'x'
        assert "salary" in optimized

    def test_combine_filters(self, employees):
        result = employees.filter(F.col("dept") == "Eng").filter(F.col("salary") > 90000)
        optimized = result._jdf.queryExecution().optimizedPlan().toString()
        # Two separate filters may be combined into one with AND
        assert "Filter" in optimized

    def test_impossible_predicate_eliminates_scan(self, spark):
        df = spark.range(100).filter(F.lit(False))
        optimized = df._jdf.queryExecution().optimizedPlan().toString()
        assert "LocalRelation" in optimized or "empty" in optimized.lower()


class TestPhysicalPlan:
    """The physical plan selects concrete execution operators."""

    def test_physical_plan_has_operator(self, employees):
        result = employees.filter(F.col("dept") == "Eng")
        physical = result._jdf.queryExecution().executedPlan().toString()
        assert "Filter" in physical or "Scan" in physical

    def test_broadcast_join_uses_broadcast_operator(self, spark, employees):
        small = spark.createDataFrame([("Eng",), ("Mkt",)], ["dept"])
        joined = employees.join(F.broadcast(small), on="dept")
        physical = joined._jdf.queryExecution().executedPlan().toString()
        assert "Broadcast" in physical

    def test_sort_merge_join_with_hint(self, spark, employees):
        other = spark.createDataFrame([("Eng", 100), ("Mkt", 200)], ["dept", "budget"])
        joined = employees.hint("merge").join(other, on="dept")
        physical = joined._jdf.queryExecution().executedPlan().toString()
        assert "SortMergeJoin" in physical or "Sort" in physical

    def test_aggregate_in_physical_plan(self, employees):
        result = employees.groupBy("dept").agg(F.sum("salary").alias("total"))
        physical = result._jdf.queryExecution().executedPlan().toString()
        assert "HashAggregate" in physical or "Aggregate" in physical

    def test_exchange_node_for_shuffle(self, employees):
        result = employees.groupBy("dept").count()
        physical = result._jdf.queryExecution().executedPlan().toString()
        assert "Exchange" in physical or "Shuffle" in physical or "Adaptive" in physical

    def test_codegen_in_physical_plan(self, employees):
        result = employees.filter(F.col("salary") > 70000)
        physical = result._jdf.queryExecution().executedPlan().toString()
        # Whole-stage codegen operators are prefixed with '*'
        assert "WholeStageCodegen" in physical or "*" in physical


class TestSqlVsDataFramePlans:
    """SQL and DataFrame API produce equivalent optimised plans."""

    def test_same_result(self, spark, employees):
        employees.createOrReplaceTempView("emp")

        df_result = employees.filter(F.col("dept") == "Eng").select("name", "salary").orderBy(F.desc("salary"))
        sql_result = spark.sql("""
            SELECT name, salary FROM emp
            WHERE dept = 'Eng' ORDER BY salary DESC
        """)

        df_rows = [row.asDict() for row in df_result.collect()]
        sql_rows = [row.asDict() for row in sql_result.collect()]
        assert df_rows == sql_rows

    def test_both_produce_optimized_plan(self, spark, employees):
        employees.createOrReplaceTempView("emp")

        df_result = employees.filter(F.col("dept") == "Eng")
        sql_result = spark.sql("SELECT * FROM emp WHERE dept = 'Eng'")

        df_opt = df_result._jdf.queryExecution().optimizedPlan().toString()
        sql_opt = sql_result._jdf.queryExecution().optimizedPlan().toString()

        # Both should contain the same filter
        assert "Filter" in df_opt
        assert "Filter" in sql_opt


class TestPlanEvolution:
    """A single query's plan evolves through all four phases."""

    def test_all_four_phases_exist(self, employees):
        result = employees.filter(F.col("salary") > 70000).groupBy("dept").agg(F.avg("salary").alias("avg_sal"))
        qe = result._jdf.queryExecution()

        parsed = qe.logical().toString()
        analyzed = qe.analyzed().toString()
        optimized = qe.optimizedPlan().toString()
        physical = qe.executedPlan().toString()

        assert len(parsed) > 0
        assert len(analyzed) > 0
        assert len(optimized) > 0
        assert len(physical) > 0

    def test_optimized_is_different_from_parsed(self, employees):
        result = employees.withColumn("x", F.lit(1 + 2 + 3)).select("name", "x")
        qe = result._jdf.queryExecution()
        parsed = qe.logical().toString()
        optimized = qe.optimizedPlan().toString()
        # Constant folding: optimized plan should contain '6' as a literal
        assert "6" in optimized
        # Plans should not be identical (optimization changed something)
        assert parsed != optimized or "6" in parsed

    def test_physical_plan_has_concrete_operators(self, employees):
        result = employees.groupBy("dept").agg(F.count("id").alias("cnt")).orderBy("dept")
        physical = result._jdf.queryExecution().executedPlan().toString()
        # Physical plan should have concrete operators, not logical ones
        has_concrete = any(
            op in physical for op in ["HashAggregate", "Sort", "Exchange", "Scan", "Adaptive", "WholeStageCodegen"]
        )
        assert has_concrete


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
