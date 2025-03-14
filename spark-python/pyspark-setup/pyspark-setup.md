# Python Version Supported

Python 3.7 and above.

# Using PyPI

      pip install pyspark

      PYSPARK_HADOOP_VERSION=2 pip install pyspark

      PYSPARK_RELEASE_MIRROR=http://mirror.apache-kr.org PYSPARK_HADOOP_VERSION=2 pip install

# It is recommended to use -v option in pip to track the installation and download status.

      PYSPARK_HADOOP_VERSION=2 pip install pyspark -v

# Supported values in PYSPARK_HADOOP_VERSION are:

without: Spark pre-built with user-provided Apache Hadoop

2: Spark pre-built for Apache Hadoop 2.7

3: Spark pre-built for Apache Hadoop 3.3 and later (default)


# Using Conda

      conda create -n pyspark_env
      conda activate pyspark_env
   
      conda install -c conda-forge pyspark 

# Manually Downloading

# 1. Install Java 8

      JAVA_HOME
      PATH

# set spark binary
Ensure the SPARK_HOME environment variable points to the directory where the tar file has been extracted.

      SPARK_HOME  = C:\apps\spark-3.0.0-bin-hadoop2.7
      HADOOP_HOME = C:\apps\spark-3.0.0-bin-hadoop2.7
      PATH=%PATH%;C:\apps\spark-3.0.0-bin-hadoop2.7\bin
      export PYTHONPATH=$(ZIPS=("$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "${ZIPS[*]}"):$PYTHONPATH

# Install winutils.exe on Windows
   
      %SPARK_HOME%\bin

# Dependencies 

| Dependencies | Package Minimum supported version | Note                                                           |
|--------------|-----------------------------------|----------------------------------------------------------------|
| pandas       | 1.0.5                             | Optional for Spark SQL                                         |
| pyarrow      | 1.0.0                             | Optional for Spark SQL                                         |
 | py4j         | 0.10.9.5                          | Required                                                       |
 | pandas       | 1.0.5                             | Required for pandas API on Spark                               |
 | pyarrow      | 1.0.0                             | Required for pandas API on Spark                               |
 | numpy        | 1.15                              | Required for pandas API on Spark and MLLib DataFrame-based API |



