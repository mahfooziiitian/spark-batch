# PySpark Script Mode — `spark-submit`

`spark-submit` is the standard way to package and ship a PySpark job to any
target environment. The script is a regular `.py` file; only the submit command
changes between environments.

## Files in this folder

| File | Description |
|------|-------------|
| `word_count.py` | Classic word-count job (beginner) |
| `sales_analysis.py` | Multi-step ETL pipeline (realistic) |

## Submit commands

### Local
```bash
spark-submit --master local[*] word_count.py
spark-submit --master local[*] sales_analysis.py
```

### YARN (from edge node)
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-cores 2 \
  --executor-memory 4g \
  sales_analysis.py
```

### Kubernetes
```bash
spark-submit \
  --master k8s://https://<api-server>:6443 \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=my-registry/pyspark:3.5 \
  sales_analysis.py
```

## Packaging dependencies

### Single extra Python file
```bash
spark-submit --master local[*] --py-files utils.py sales_analysis.py
```

### Zip of a package
```bash
zip -r mylib.zip mylib/
spark-submit --master local[*] --py-files mylib.zip sales_analysis.py
```

### PyPI packages (via `--packages`)
```bash
spark-submit --master local[*] \
  --packages com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.hadoop:hadoop-aws:3.3.4 \
  sales_analysis.py
```

## Environment variables

Set these before calling `spark-submit`:

```bash
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
```

