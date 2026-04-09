import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def _print_plan(title: str, plan_str: str) -> None:
    """Print a plan phase with a header."""
    separator = "-" * 60
    print(f"\n{separator}")
    print(f"  {title}")
    print(separator)
    print(plan_str)


def _build_sample_df(spark: SparkSession):
    """Build a sample DataFrame used across demos."""
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("department", StringType()),
            StructField("salary", DoubleType()),
        ]
    )
    data = [
        (1, "Alice", "Engineering", 95000.0),
        (2, "Bob", "Marketing", 72000.0),
        (3, "Charlie", "Engineering", 110000.0),
        (4, "Diana", "Marketing", 68000.0),
        (5, "Eve", "Engineering", 105000.0),
        (6, "Frank", "Sales", 60000.0),
        (7, "Grace", "Sales", 65000.0),
        (8, "Hank", "Engineering", 98000.0),
    ]
    return spark.createDataFrame(data, schema)


def demo_four_plan_phases(spark: SparkSession) -> None:
    """Extract and display each of the four Catalyst plan phases.

    1. Parsed Logical Plan   — raw AST from user code, unresolved references
    2. Analyzed Logical Plan  — column names, types, and table refs resolved
    3. Optimized Logical Plan — rule-based and cost-based rewrites applied
    4. Physical Plan          — concrete operators chosen (scan type, join algo)
    """
    df = _build_sample_df(spark)
    result = (
        df.filter(F.col("department") == "Engineering")
        .groupBy("department")
        .agg(
            F.count("id").alias("headcount"),
            F.round(F.avg("salary"), 2).alias("avg_salary"),
        )
    )

    qe = result._jdf.queryExecution()

    _print_plan("1. PARSED LOGICAL PLAN", qe.logical().toString())
    _print_plan("2. ANALYZED LOGICAL PLAN", qe.analyzed().toString())
    _print_plan("3. OPTIMIZED LOGICAL PLAN", qe.optimizedPlan().toString())
    _print_plan("4. PHYSICAL PLAN", qe.executedPlan().toString())


def demo_parsed_plan(spark: SparkSession) -> None:
    """The parsed plan is the raw logical tree before any resolution.

    Column and table references may be unresolved at this stage.
    The parser translates DataFrame API calls (or SQL text) into a tree of
    logical operators: Project, Filter, Aggregate, etc.
    """
    df = _build_sample_df(spark)
    result = df.select("name", "salary").filter(F.col("salary") > 80000)

    parsed = result._jdf.queryExecution().logical().toString()
    _print_plan("PARSED LOGICAL PLAN", parsed)

    print("\nWhat to look for:")
    print("  - 'Filter' node with the predicate (salary > 80000)")
    print("  - 'Project' node listing only [name, salary]")
    print("  - References may show as 'unresolvedattribute' in SQL-parsed plans")


def demo_analyzed_plan(spark: SparkSession) -> None:
    """The analyzed plan resolves all column names, data types, and references.

    The Analyzer uses the catalog to:
    - Resolve column names to their source tables
    - Assign data types to every expression
    - Validate that operations are type-compatible
    - Expand star (*) references into explicit column lists
    """
    df = _build_sample_df(spark)

    # Star expansion: select(*) expands to all columns
    star_result = df.select("*")
    analyzed_star = star_result._jdf.queryExecution().analyzed().toString()
    _print_plan("ANALYZED PLAN — star expansion", analyzed_star)

    # Type resolution: cast + comparison
    typed_result = df.filter(F.col("salary").cast("int") > 80000)
    analyzed_typed = typed_result._jdf.queryExecution().analyzed().toString()
    _print_plan("ANALYZED PLAN — type resolution", analyzed_typed)

    print("\nWhat to look for:")
    print("  - Every column has a resolved type (e.g., salary#4: double)")
    print("  - Star (*) is expanded to [id, name, department, salary]")
    print("  - Cast operations appear explicitly in the tree")


def demo_optimized_plan(spark: SparkSession) -> None:
    """The optimized plan applies rule-based rewrites to reduce work.

    Key optimization rules:
    - Predicate Pushdown: move filters closer to the data source
    - Column Pruning: drop columns not needed downstream
    - Constant Folding: evaluate literal expressions at plan time
    - Combine Filters: merge consecutive filter nodes
    - Boolean Simplification: simplify always-true/false predicates
    """
    df = _build_sample_df(spark)

    # Predicate pushdown: filter written after projection moves before it
    pushdown = df.withColumn("bonus", F.col("salary") * 0.1).filter(F.col("department") == "Engineering")

    analyzed = pushdown._jdf.queryExecution().analyzed().toString()
    optimized = pushdown._jdf.queryExecution().optimizedPlan().toString()
    _print_plan("ANALYZED (before optimization)", analyzed)
    _print_plan("OPTIMIZED (after predicate pushdown)", optimized)

    print("\nWhat to look for:")
    print("  - In analyzed: Filter is above the Project (bonus)")
    print("  - In optimized: Filter is pushed below the Project")

    # Column pruning: unused columns dropped
    pruned = df.select("name")
    opt_pruned = pruned._jdf.queryExecution().optimizedPlan().toString()
    _print_plan("OPTIMIZED — column pruning", opt_pruned)
    print("  - Only 'name' appears in the scan; id, department, salary are pruned")

    # Constant folding: literal math evaluated at plan time
    folded = df.withColumn("const", F.lit(365 * 24 * 60))
    opt_folded = folded._jdf.queryExecution().optimizedPlan().toString()
    _print_plan("OPTIMIZED — constant folding", opt_folded)
    print("  - The expression (365 * 24 * 60) becomes the literal 525600")

    # Boolean simplification
    simplified = df.filter(F.lit(True) & (F.col("salary") > 0))
    opt_simplified = simplified._jdf.queryExecution().optimizedPlan().toString()
    _print_plan("OPTIMIZED — boolean simplification", opt_simplified)
    print("  - 'true AND (salary > 0)' simplified to just '(salary > 0)'")


def demo_physical_plan(spark: SparkSession) -> None:
    """The physical plan selects concrete execution operators.

    Decisions made at this stage:
    - Join strategy: BroadcastHashJoin vs SortMergeJoin vs ShuffleHashJoin
    - Scan type: InMemoryTableScan, FileScan, ExternalRDDScan
    - Exchange (shuffle) placement
    - Whole-stage code generation (codegen) boundaries
    """
    df = _build_sample_df(spark)
    small = spark.createDataFrame([(d,) for d in ["Engineering", "Sales"]], ["dept"])

    # Broadcast hash join — small table broadcast
    broadcast_join = df.join(F.broadcast(small), df.department == small.dept)
    physical_bc = broadcast_join._jdf.queryExecution().executedPlan().toString()
    _print_plan("PHYSICAL — BroadcastHashJoin", physical_bc)

    # Sort-merge join — hint forces SortMerge even for small data
    smj = df.hint("merge").join(small, df.department == small.dept)
    physical_smj = smj._jdf.queryExecution().executedPlan().toString()
    _print_plan("PHYSICAL — SortMergeJoin (hinted)", physical_smj)

    # Aggregation with whole-stage codegen
    agg = df.groupBy("department").agg(F.sum("salary").alias("total"))
    physical_agg = agg._jdf.queryExecution().executedPlan().toString()
    _print_plan("PHYSICAL — Aggregate with codegen", physical_agg)

    print("\nWhat to look for:")
    print("  - 'BroadcastHashJoin' vs 'SortMergeJoin' operator names")
    print("  - 'Exchange' nodes indicate shuffle boundaries")
    print("  - '*' prefix on operators indicates whole-stage codegen enabled")
    print("  - 'AdaptiveSparkPlan' wrapper when AQE is active")


def demo_sql_vs_dataframe_plans(spark: SparkSession) -> None:
    """SQL and DataFrame API produce identical optimized plans."""
    df = _build_sample_df(spark)
    df.createOrReplaceTempView("employees")

    # DataFrame API
    df_result = df.filter(F.col("department") == "Engineering").select("name", "salary").orderBy(F.desc("salary"))

    # SQL API — same query
    sql_result = spark.sql("""
        SELECT name, salary
        FROM employees
        WHERE department = 'Engineering'
        ORDER BY salary DESC
    """)

    df_opt = df_result._jdf.queryExecution().optimizedPlan().toString()
    sql_opt = sql_result._jdf.queryExecution().optimizedPlan().toString()

    _print_plan("OPTIMIZED — DataFrame API", df_opt)
    _print_plan("OPTIMIZED — SQL API", sql_opt)

    # Verify both produce the same result
    df_rows = [row.asDict() for row in df_result.collect()]
    sql_rows = [row.asDict() for row in sql_result.collect()]
    match = df_rows == sql_rows
    print(f"\nSame result: {match}")
    print("Both APIs go through the same Catalyst pipeline and produce equivalent plans.")


def demo_plan_comparison(spark: SparkSession) -> None:
    """Show how the plan transforms at each phase for a multi-step query."""
    df = _build_sample_df(spark)

    query = (
        df.filter(F.col("salary") > 70000)
        .withColumn("tax", F.col("salary") * 0.3)
        .select("name", "department", "salary", "tax")
        .orderBy("department", F.desc("salary"))
    )

    qe = query._jdf.queryExecution()

    print("=== Plan Evolution ===")
    print("Query: filter(salary > 70000) → withColumn(tax) → select → orderBy")

    _print_plan("PARSED", qe.logical().toString())
    _print_plan("ANALYZED", qe.analyzed().toString())
    _print_plan("OPTIMIZED", qe.optimizedPlan().toString())
    _print_plan("PHYSICAL", qe.executedPlan().toString())

    print(f"\nResult rows: {query.count()}")
    query.show()


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("query-plans-demo")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=== Four Plan Phases ===")
    demo_four_plan_phases(spark)
    print("\n\n=== Parsed Plan ===")
    demo_parsed_plan(spark)
    print("\n\n=== Analyzed Plan ===")
    demo_analyzed_plan(spark)
    print("\n\n=== Optimized Plan ===")
    demo_optimized_plan(spark)
    print("\n\n=== Physical Plan ===")
    demo_physical_plan(spark)
    print("\n\n=== SQL vs DataFrame Plans ===")
    demo_sql_vs_dataframe_plans(spark)
    print("\n\n=== Plan Comparison (Full Pipeline) ===")
    demo_plan_comparison(spark)
    spark.stop()
