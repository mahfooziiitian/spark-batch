# :material-play-circle: SQL Execution in Spark

This section connects the high-level Spark SQL pipeline to the runtime details you actually inspect: lazy `DataFrame` construction, physical plans, jobs, stages, tasks, and `EXPLAIN` output.

### :material-animation-play: Interactive Visualization — Lazy Plan vs Action Trigger

<div id="viz-execution-lazy-action" class="ts-viz"></div>

This visualization shows the verified boundary between planning and execution: transformations extend a plan, `EXPLAIN` inspects it, and an action such as `collect()` is what turns the plan into scheduled work.

______________________________________________________________________

## :material-sitemap: Two Timelines to Keep Straight

Spark SQL has two related timelines:

| Timeline                 | Question it answers                                                                               | Typical evidence                       |
| ------------------------ | ------------------------------------------------------------------------------------------------- | -------------------------------------- |
| SQL clause semantics     | In what logical order are `FROM`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, and `LIMIT` applied? | Query results and logical plans        |
| Spark execution pipeline | How does `spark.sql(...)` become jobs, stages, and tasks?                                         | `EXPLAIN`, Spark UI, `statusTracker()` |

The order you write clauses is not the same as the order Spark compiles and executes them.

```sql
SELECT
    country,
    AVG(salary) AS avg_salary
FROM employees
WHERE age > 30
GROUP BY country
HAVING AVG(salary) > 50000
ORDER BY avg_salary DESC
LIMIT 10;
```

| Logical step | Clause          | What Spark is deciding                                    |
| ------------ | --------------- | --------------------------------------------------------- |
| 1            | `FROM` / `JOIN` | Which relations to scan and how to join them              |
| 2            | `WHERE`         | Which row-level predicates can be applied early           |
| 3            | `GROUP BY`      | Which keys define aggregation groups                      |
| 4            | `HAVING`        | Which aggregate results survive                           |
| 5            | `SELECT`        | Which expressions become output columns                   |
| 6            | `ORDER BY`      | Whether a global ordering step is required                |
| 7            | `LIMIT`         | Whether Spark can use a top-N shortcut or must trim later |

!!! note "Alias visibility"

    `SELECT` aliases are not available in `WHERE`, because `WHERE` is evaluated first. Use a CTE or subquery if a later alias must be filtered.

______________________________________________________________________

## :material-information-outline: From `spark.sql(...)` to running tasks

A `SparkSession` parses SQL immediately, but execution stays lazy:

1. `spark.sql("...")` returns a `DataFrame` backed by a logical plan.
2. Further transformations (`select`, `where`, `groupBy`, `join`) add to that plan.
3. `explain()` asks Spark to compile the plan, but not to run it.
4. An action such as `.show()`, `.collect()`, `.count()`, or `.write...` is what hands work to the scheduler.

### :material-flask-outline: Verified example — `explain()` is not an action

Verified with PySpark 4.2 by defining a Python UDF that always raises:

```python
from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

@F.udf(returnType=T.LongType())
def boom(x):
    raise RuntimeError("boom during execution")

df = spark.range(3).select(boom("id").alias("boom"))
df.explain("formatted")   # succeeds: Spark only plans the query
# df.collect()             # fails: Spark finally executes the Python UDF

spark.stop()
```

Observed behavior in Spark 4.2:

- `df.explain("formatted")` produced a physical plan containing `ArrowEvalPython` and returned successfully.
- `df.collect()` failed with `PythonException`, proving the UDF did not run during planning.

That is the practical boundary between **plan inspection** and **query execution**.

______________________________________________________________________

## :material-map-legend: A verified end-to-end execution sketch

For a real shuffle-producing query such as:

```sql
SELECT
    id % 5 AS g,
    COUNT(*) AS c
FROM range(1000)
WHERE id >= 100
GROUP BY id % 5;
```

Spark 4.2 builds a pipeline like this:

1. **Range scan** creates the input rows.
2. **Filter** keeps only `id >= 100`.
3. **Project** computes the grouping key `id % 5`.
4. **Partial `HashAggregate`** counts rows per key inside each input partition.
5. **`Exchange hashpartitioning(...)`** shuffles rows so matching keys meet.
6. **Final `HashAggregate`** merges the partial counts.
7. **Action** returns rows to the driver.

A representative physical-plan excerpt is:

```text
*(2) HashAggregate(keys=[_groupingexpression#...], functions=[count(1)])
+- Exchange hashpartitioning(_groupingexpression#..., 200), ENSURE_REQUIREMENTS
   +- *(1) HashAggregate(keys=[_groupingexpression#...], functions=[partial_count(1)])
      +- *(1) Project [(id#... % 5) AS _groupingexpression#...]
         +- *(1) Filter (id#... >= 100)
            +- *(1) Range (0, 1000, step=1, splits=2)
```

Two useful rules follow directly from the verified plan:

- `Exchange` is the clearest sign that Spark must repartition data and create a new stage boundary.
- `*(N)` markers show which operators participate in whole-stage code generation.

______________________________________________________________________

## :material-map-legend: How this folder fits with the planner internals docs

This folder stays complementary to the deeper compiler pages elsewhere in the site:

- [Query Lifecycle](query-lifecycle.md) focuses on how physical plans become jobs, stages, and tasks.
- [EXPLAIN Plans](explain.md) focuses on reading Spark's plan output accurately.
- [Query Parsing & Execution](../../internals/planner/query-parsing.md) covers parser → analyzer → optimizer pipeline details.
- [Query Planner](../../internals/planner/query-planner.md) focuses on physical-strategy selection such as broadcast vs sort-merge joins.

If you need compiler internals, follow the planner pages above. If you need runtime observability, stay in this folder.

______________________________________________________________________

## :material-play-circle: Run

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.sql("SELECT id % 5 AS g, COUNT(*) AS c FROM range(1000) GROUP BY id % 5")
df.explain("formatted")
print(df.collect())

spark.stop()
```
