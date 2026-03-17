# PySpark in Jupyter Notebooks

Notebooks are ideal for exploratory data analysis (EDA), prototyping, and
sharing results with stakeholders.

## Prerequisites

```bash
pip install pyspark jupyter findspark
```

## Option A — `PYSPARK_DRIVER_PYTHON` (recommended)

Let PySpark launch Jupyter automatically:

```bash
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --port=8888"

pyspark --master local[*]
```

Open the URL shown in the terminal. `spark` and `sc` are pre-injected.

## Option B — `findspark` inside an existing Jupyter server

Start Jupyter normally, then use `findspark` in the first cell:

```python
import findspark
findspark.init()          # auto-detects SPARK_HOME

from pyspark.sql import SparkSession
spark = (SparkSession.builder
         .appName("notebook-eda")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
```

## Option C — PySpark built-in kernel (Spark 3.4+)

```bash
pip install "pyspark[connect]"
```

Then in a cell:
```python
from pyspark.sql.connect.session import SparkSession
spark = SparkSession.builder.remote("sc://localhost").getOrCreate()
```

## Notebook file

See **[pyspark_notebook.ipynb](pyspark_notebook.ipynb)** for a full EDA
example you can open directly in JupyterLab.

## Tips

- Use `spark.stop()` in the last cell to release resources cleanly.
- Call `display(df)` in Databricks notebooks for a rich interactive table.
- Keep `spark.sql.shuffle.partitions` low (4–8) for notebook-scale data.

