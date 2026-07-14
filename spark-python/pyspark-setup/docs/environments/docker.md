# Docker

Run PySpark inside a Docker container for a fully reproducible, dependency-free
development environment.

## How it works

```mermaid
graph LR
    A[Host machine] -->|docker run| B[pyspark-dev container]
    B -->|mounts| C[/workspace = your code]
    B -->|port 4040| D[Spark UI]
```

## Build the Image

```bash
docker build -t pyspark-dev:3.5 docker/
```

The `docker/Dockerfile` is based on `apache/spark:3.5.0-python3` and adds:

- `pyarrow`, `pandas`, `numpy`
- A `python → python3` symlink
- `PYTHONPATH` wired to the Spark Python bindings (required for `python3 script.py`)
- `SPARK_LOCAL_IP=127.0.0.1` to avoid container DNS issues

## Run a Script

```bash
docker run --rm \
  -v "$(pwd)":/workspace \
  pyspark-dev:3.5 python3 /workspace/docker/docker_example.py
```

## Interactive Shell

```bash
docker run --rm -it \
  -v "$(pwd)":/workspace \
  -p 4040:4040 \
  pyspark-dev:3.5 bash
```

Then inside the container:

```bash
python3 /workspace/docker/docker_example.py
# or launch pyspark REPL:
pyspark
```

## PYTHONPATH — Why It Matters

The `apache/spark` entrypoint only configures `PYTHONPATH` when launching via
`spark-submit` or `pyspark`. Running `python3 script.py` directly bypasses this.
The Dockerfile sets it explicitly:

```dockerfile
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"
```

!!! note "Upgrading Spark"
    Update the `py4j` version in the `ENV` line whenever you change the base image tag.

## Run the Example

```bash
docker build -t pyspark-dev:3.5 docker/
docker run --rm -v "$(pwd)":/workspace \
  pyspark-dev:3.5 python3 /workspace/docker/docker_example.py
```

## Full Example

```python title="docker/docker_example.py"
--8<-- "docker/docker_example.py"
```
