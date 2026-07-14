# Notebook

Jupyter notebooks combine live PySpark code, markdown prose, and output in a
single shareable document — perfect for EDA, prototyping, and presenting results.

## Prerequisites

=== "pip"
    ```bash
    pip install pyspark jupyter findspark
    ```

=== "uv"
    ```bash
    uv add pyspark jupyter findspark
    ```

## Setup options

=== "Option A — pyspark CLI (recommended)"
    Let PySpark launch Jupyter automatically. `spark` and `sc` are pre-injected
    into every notebook kernel.

    ```bash
    export PYSPARK_DRIVER_PYTHON=jupyter
    export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --port=8888"

    pyspark --master local[*]
    ```

    Open the URL shown in the terminal — no extra imports needed.

=== "Option B — findspark"
    Start a regular Jupyter session, then initialise Spark in the first cell:

    ```python
    import findspark
    findspark.init()   # auto-detects SPARK_HOME

    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
             .appName("notebook-eda")
             .master("local[*]")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    ```

=== "Option C — Spark Connect (3.4+)"
    The lightest option: no JVM in the notebook process.

    ```bash
    pip install "pyspark[connect]"
    ```

    ```python
    from pyspark.sql.connect.session import SparkSession
    spark = SparkSession.builder.remote("sc://localhost").getOrCreate()
    ```

## Example notebook

Open **`notebook/pyspark_notebook.ipynb`** in JupyterLab for a complete EDA session covering:

- DataFrame creation and schema inspection
- Descriptive statistics with `.describe()`
- Aggregation and groupBy
- SQL on temp views
- Window functions with `RANK()`
- Parquet write and read-back

## Tips

!!! tip "Always stop the session"
    Call `spark.stop()` in the last cell to release JVM resources cleanly.

!!! tip "Keep shuffle partitions low"
    Notebook data is rarely large. Set `spark.sql.shuffle.partitions` to `4`–`8`
    to avoid 200 empty shuffle files on every aggregation.

!!! note "Databricks notebooks"
    On Databricks, `spark` is injected automatically and `display(df)` renders
    a rich interactive table instead of `.show()`.
