# Spark env

Both executor and driver should have the same python version.

## Driver python

    export PYSPARK_DRIVER_PYTHON=python3

## Executor Python

    export PYSPARK_PYTHON=python3


## Supported pyspark, python and java version

| pyspark     | python           | java         | scala           |
|-------------|------------------|--------------|-----------------|
| Spark 3.2.4 | Python 3.6+      |              | Scala 2.12/2.13 |
| Spark 3.5.0 | Python 3.8+      | Java 8/11/17 | Scala 2.12/2.13 |
| Spark 2.4.7 | Python 2.7+/3.4+ | Java 8       | Scala 2.12      |
