# Conda

Install PySpark inside a Conda environment. Conda manages both the Python
interpreter and the Java runtime in a single, reproducible environment.

## Prerequisites

Install [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or
[Anaconda](https://www.anaconda.com/download) first.

## Setup

=== "From YAML (recommended)"
    ```bash
    conda env create -f conda/environment.yml
    conda activate pyspark-env
    ```

=== "Manual"
    ```bash
    conda create -n pyspark-env python=3.11
    conda activate pyspark-env
    conda install -c conda-forge pyspark=3.5.0 pyarrow pandas numpy jupyter
    ```

The provided [`conda/environment.yml`](../conda/environment.yml) pins all versions and
includes `openjdk=11` so Java is managed by Conda as well.

## findspark (optional)

`findspark` helps locate Spark when `SPARK_HOME` is not explicitly set:

```bash
pip install findspark
```

```python
import findspark
findspark.init()   # call before any pyspark import

from pyspark.sql import SparkSession
```

## SparkSession

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("conda-job")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
```

## Run the Example

```bash
conda activate pyspark-env
python conda/conda_example.py
```

## Full Example

```python title="conda/conda_example.py"
--8<-- "conda/conda_example.py"
```
