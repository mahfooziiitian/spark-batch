# PySpark Setup

> Tested with **PySpark 3.5.x** · Python 3.8 – 3.12 · Java 8 / 11 / 17

---

## Version Compatibility

| PySpark | Python       | Java         | Scala           |
|---------|--------------|--------------|-----------------|
| 3.5.x   | 3.8 – 3.12   | 8 / 11 / 17  | 2.12 / 2.13     |
| 3.4.x   | 3.7 – 3.11   | 8 / 11 / 17  | 2.12 / 2.13     |
| 3.3.x   | 3.7 – 3.10   | 8 / 11       | 2.12 / 2.13     |
| 3.2.x   | 3.6 – 3.10   | 8 / 11       | 2.12 / 2.13     |
| 2.4.x   | 2.7+ / 3.4+  | 8            | 2.12            |

---

## Quick Start

```bash
# 1 – Install Java 11 (see Prerequisites below)
# 2 – Install Python 3.8+
pip install "pyspark==3.5.0"

# Verify
python -c "import pyspark; print(pyspark.__version__)"
```

---

## Environments

| Environment               | Guide                                           |
|---------------------------|-------------------------------------------------|
| [Local / venv](#local--venv)  | Laptop / workstation development            |
| [Conda](#conda)           | Anaconda / Miniconda                            |
| [Docker](#docker)         | Isolated container                              |
| [YARN](#yarn)             | On-premise Hadoop cluster                       |
| [Kubernetes](#kubernetes) | Cloud-native / containerised                    |
| [AWS EMR](#aws-emr)       | Managed Spark on EC2                            |
| [AWS Glue](#aws-glue)     | Serverless ETL service                          |
| [CI/CD](#cicd)            | GitHub Actions                                  |

---

## Prerequisites

### Java

PySpark requires Java 8, 11, or 17. **Java 11 LTS is recommended.**

```bash
# macOS
brew install openjdk@11
echo 'export JAVA_HOME=$(brew --prefix openjdk@11)' >> ~/.zshrc

# Ubuntu / Debian
sudo apt-get update && sudo apt-get install -y openjdk-11-jdk
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))

# Windows (PowerShell)
winget install EclipseAdoptium.Temurin.11.JDK

# Verify
java -version
```

### Key Environment Variables

```bash
# Required for all environments
export JAVA_HOME=/path/to/jdk
export PYSPARK_PYTHON=python3               # Python interpreter for executors
export PYSPARK_DRIVER_PYTHON=python3        # Python interpreter for the driver

# Spark binary (only needed for tarball install)
export SPARK_HOME=/opt/spark
export PATH="$SPARK_HOME/bin:$PATH"

# YARN / Hadoop
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf

# Helps avoid hostname-resolution issues locally
export SPARK_LOCAL_IP=127.0.0.1

# Optional
export SPARK_WAREHOUSE=/tmp/spark-warehouse
export DERBY_HOME=/tmp/derby
```

---

## Local / venv

```bash
# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# Install PySpark and common data dependencies
pip install -r local/requirements.txt

# Point PySpark at the venv Python
export PYSPARK_PYTHON=$(which python)
export PYSPARK_DRIVER_PYTHON=$(which python)

# Quick smoke test
python - <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
print("Spark", spark.version, "ready")
spark.stop()
EOF
```

Use [`local/setup-venv.sh`](local/setup-venv.sh) for a one-command setup:

```bash
bash local/setup-venv.sh          # creates .venv, installs deps, runs smoke test
```

---

## Conda

```bash
# Create environment from the provided YAML (recommended)
conda env create -f conda/environment.yml
conda activate pyspark-env

# Or install manually
conda create -n pyspark-env python=3.11
conda activate pyspark-env
conda install -c conda-forge pyspark=3.5.0 pyarrow pandas numpy jupyter
```

See [`conda/environment.yml`](conda/environment.yml).

---

## Docker

```bash
# Build the development image
docker build -t pyspark-dev:3.5 docker/

# Run interactively (mounts current directory, exposes Spark UI on port 4040)
docker run --rm -it \
  -v "$(pwd)":/workspace \
  -p 4040:4040 \
  pyspark-dev:3.5 bash

# Run a specific script
docker run --rm \
  -v "$(pwd)":/workspace \
  pyspark-dev:3.5 python3 /workspace/my_job.py
```

See [`docker/Dockerfile`](docker/Dockerfile).

---

## YARN

```bash
# 1. Set Hadoop / YARN environment variables
export HADOOP_CONF_DIR=/etc/hadoop/conf   # must contain core-site.xml, yarn-site.xml

# 2. Install PySpark on the edge node
pip install pyspark==3.5.0

# 3. Use the same Python binary on both driver and executors
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# 4. Test HDFS connectivity
hdfs dfs -ls /

# 5. Submit a test job
spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 2 \
  --executor-memory 2g \
  my_job.py
```

Source [`cluster/setup-yarn-env.sh`](cluster/setup-yarn-env.sh) to bootstrap all required variables at once:

```bash
source cluster/setup-yarn-env.sh
```

### Shipping Python dependencies to YARN executors

```bash
# Pack the active virtualenv into a tarball
pip install venv-pack
venv-pack -o pyspark_venv.tar.gz

spark-submit \
  --master yarn \
  --archives pyspark_venv.tar.gz#env \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./env/bin/python \
  --conf spark.executorEnv.PYSPARK_PYTHON=./env/bin/python \
  my_job.py
```

---

## Kubernetes

```bash
# 1. Ensure kubectl is configured
kubectl cluster-info

# 2. Create the Spark service account (once per namespace)
kubectl create serviceaccount spark
kubectl create clusterrolebinding spark-role \
  --clusterrole=edit \
  --serviceaccount=default:spark \
  --namespace=default

# 3. Build and push your Docker image
docker build -t my-registry/pyspark-job:3.5 docker/
docker push my-registry/pyspark-job:3.5

# 4. Submit
spark-submit \
  --master k8s://https://$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}') \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=my-registry/pyspark-job:3.5 \
  --conf spark.kubernetes.namespace=default \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.executor.instances=3 \
  local:///opt/spark/work-dir/my_job.py
```

---

## AWS EMR

```bash
# Upload script to S3
aws s3 cp my_job.py s3://my-bucket/scripts/

# Create cluster
aws emr create-cluster \
  --name "PySpark Job" \
  --release-label emr-7.1.0 \
  --applications Name=Spark \
  --instance-type m5.xlarge \
  --instance-count 3 \
  --use-default-roles \
  --log-uri s3://my-bucket/logs/ \
  --region us-east-1

# Submit as an EMR Step
aws emr add-steps \
  --cluster-id j-XXXXXXXXXXXX \
  --steps Type=Spark,Name="ETL",ActionOnFailure=CONTINUE,\
Args=[--deploy-mode,cluster,\
      --conf,spark.sql.adaptive.enabled=true,\
      s3://my-bucket/scripts/my_job.py]
```

### PYSPARK_HADOOP_VERSION (PyPI install variants)

```bash
# Default — Hadoop 3.3+ (recommended)
pip install pyspark==3.5.0

# Hadoop 2.7 (legacy clusters)
PYSPARK_HADOOP_VERSION=2 pip install pyspark==3.5.0

# No bundled Hadoop (use cluster's own Hadoop)
PYSPARK_HADOOP_VERSION=without pip install pyspark==3.5.0

# Use a specific mirror with verbose output
PYSPARK_RELEASE_MIRROR=http://mirror.apache-kr.org \
PYSPARK_HADOOP_VERSION=3 \
pip install pyspark -v
```

---

## AWS Glue

```bash
# Install Glue local libraries for unit testing
pip install aws-glue-sessions

# Create a Glue job
aws glue create-job \
  --name my-pyspark-glue-job \
  --role AWSGlueServiceRole \
  --command '{"Name":"glueetl","ScriptLocation":"s3://my-bucket/scripts/my_job.py","PythonVersion":"3"}' \
  --default-arguments '{
    "--job-language": "python",
    "--enable-metrics": "",
    "--enable-continuous-cloudwatch-log": "true"
  }' \
  --glue-version "4.0" \
  --number-of-workers 5 \
  --worker-type G.1X

# Start a run
aws glue start-job-run --job-name my-pyspark-glue-job
```

---

## CI/CD

See [`ci/github-actions.yml`](ci/github-actions.yml) for a ready-to-use GitHub Actions workflow that:

- Tests against Python 3.10, 3.11, and 3.12
- Caches pip dependencies between runs
- Installs Java 11 (Temurin) via `setup-java`
- Configures `PYSPARK_PYTHON` and `SPARK_LOCAL_IP`

Copy it to `.github/workflows/pyspark.yml` in your repository.

---

## Dependencies

| Package      | Min version | Notes                                      |
|--------------|-------------|--------------------------------------------|
| `py4j`       | 0.10.9.7    | Required — bridges Python and JVM          |
| `pyarrow`    | 4.0.0       | Required for pandas API / Arrow columnar   |
| `pandas`     | 1.0.5       | Required for pandas API on Spark           |
| `numpy`      | 1.15        | Required for MLlib DataFrame-based API     |
| `findspark`  | 2.0.0       | Optional — locates Spark for plain scripts |

---

## Troubleshooting

### `JAVA_HOME is not set`

```bash
# Linux / macOS
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))

# macOS (Homebrew)
export JAVA_HOME=$(brew --prefix openjdk@11)

# Windows (PowerShell)
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-11.x-hotspot"
```

### `Python in worker has different version than driver`

Driver and executor must use the **same** Python binary:

```bash
export PYSPARK_PYTHON=$(which python3)
export PYSPARK_DRIVER_PYTHON=$(which python3)
```

### `ModuleNotFoundError` on executors (cluster mode)

```bash
# Zip extra pure-Python packages
zip -r deps.zip my_package/
spark-submit --master yarn --py-files deps.zip my_job.py

# Or ship an entire virtualenv
venv-pack -o pyspark_venv.tar.gz
spark-submit --master yarn \
  --archives pyspark_venv.tar.gz#env \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./env/bin/python \
  my_job.py
```

### Behind a corporate proxy

```bash
pip install pyspark --proxy="http://proxy-host:port"

# Or set globally
export HTTP_PROXY=http://proxy-host:port
export HTTPS_PROXY=http://proxy-host:port
```

### Windows — `winutils.exe` not found

Download `winutils.exe` for your Hadoop version from
[steveloughran/winutils](https://github.com/steveloughran/winutils) and place it in
`%SPARK_HOME%\bin`:

```powershell
$env:HADOOP_HOME = "C:\apps\spark-3.5.0-bin-hadoop3"
$env:PATH       += ";$env:HADOOP_HOME\bin"
```
