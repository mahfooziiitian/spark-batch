# Docker

Run PySpark tests in a containerised environment using Docker.

## Files

### Dockerfile

```dockerfile title="spark_docker/Dockerfile"
--8<-- "spark_docker/Dockerfile"
```

### docker-compose.yml

```yaml title="spark_docker/docker-compose.yml"
--8<-- "spark_docker/docker-compose.yml"
```

## Usage

### Build the image

```bash
cd spark_docker
docker build -t spark-test .
```

### Run tests

```bash
docker-compose up
```

This mounts the source code and runs `python3 -m pytest` inside the container.

## Environment

| Component | Version |
| --- | --- |
| Base image | Ubuntu 18.04 |
| Java | Default JDK |
| Spark | 3.0.1 |
| Python | 3.8 |
| Hadoop | 3.2 |

!!! warning "Version mismatch"
    The Docker image uses Spark 3.0.1 while the project uses PySpark 3.5.x locally.
    Consider updating the Dockerfile to match.

## Recommended Dockerfile Update

For consistency with the local environment, use the official Spark image:

```dockerfile
FROM apache/spark:3.5.0-python3

USER root
RUN pip install --no-cache-dir pytest faker pandas pyarrow

ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_LOCAL_IP=127.0.0.1

WORKDIR /workspace
USER spark
```
