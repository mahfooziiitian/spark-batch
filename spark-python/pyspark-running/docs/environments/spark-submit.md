# spark-submit

`spark-submit` is the standard tool for packaging and shipping a PySpark job to any
target environment. Your application is a plain `.py` file; only the submit command
changes between environments.

## Submit commands

=== "Local"
    ```bash
    spark-submit --master local[*] spark-submit/word_count.py
    spark-submit --master local[*] spark-submit/sales_analysis.py
    ```

=== "YARN"
    ```bash
    spark-submit \
      --master yarn \
      --deploy-mode cluster \
      --num-executors 4 \
      --executor-cores 2 \
      --executor-memory 4g \
      spark-submit/sales_analysis.py
    ```

=== "Kubernetes"
    ```bash
    spark-submit \
      --master k8s://https://<api-server>:6443 \
      --deploy-mode cluster \
      --conf spark.kubernetes.container.image=my-registry/pyspark:3.5 \
      spark-submit/sales_analysis.py
    ```

## Packaging Python dependencies

=== "Single .py file"
    ```bash
    spark-submit --master local[*] \
        --py-files utils.py \
        spark-submit/sales_analysis.py
    ```

=== "Zip package"
    ```bash
    zip -r mylib.zip mylib/
    spark-submit --master local[*] \
        --py-files mylib.zip \
        spark-submit/sales_analysis.py
    ```

=== "PyPI via --packages"
    ```bash
    spark-submit --master local[*] \
      --packages \
        com.amazonaws:aws-java-sdk-bundle:1.12.262,\
        org.apache.hadoop:hadoop-aws:3.3.4 \
      spark-submit/sales_analysis.py
    ```

=== "Virtualenv on YARN"
    ```bash
    venv-pack -o pyspark_env.tar.gz
    spark-submit \
      --master yarn \
      --archives pyspark_env.tar.gz#environment \
      --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
      spark-submit/sales_analysis.py
    ```

## Environment variables

Set these before calling `spark-submit`:

```bash
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
```

## Script design tips

!!! note "Don't hard-code the master"
    Leave `.master()` out of `SparkSession.builder` — let `spark-submit --master`
    inject it. This keeps the same script runnable on every target environment.

    ```python
    # Good — master comes from spark-submit
    spark = SparkSession.builder.appName("my-job").getOrCreate()

    # Avoid — ties the script to one environment
    spark = SparkSession.builder.master("local[*]").appName("my-job").getOrCreate()
    ```

!!! tip "Use environment variables for paths"
    Paths differ between local and cluster environments. Drive them with env vars:
    ```python
    import os
    INPUT_PATH  = os.environ.get("INPUT_PATH",  "/tmp/input")
    OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/output")
    ```

## Examples in this project

| File | Description |
|------|-------------|
| [`word_count.py`](../examples/word_count.md) | Classic word-count — beginner |
| [`sales_analysis.py`](../examples/sales_analysis.md) | Multi-step ETL pipeline — realistic |
