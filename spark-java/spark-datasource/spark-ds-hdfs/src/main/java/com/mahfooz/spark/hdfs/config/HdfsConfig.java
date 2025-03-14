/*

You need to include two Hadoop configuration files on Spark's classpath:

    a)  hdfs-site.xml

        It provides default behaviors for the HDFS client.

    b)  core-site.xml

        It sets the default file system name.

    HADOOP_CONF_DIR
    SPARK_HOME

    spark.executor.memory
    spark.executor.cores
    spark.cores.max/spark.executor.cores
    export SPARK_EXECUTOR_URI

 */
package com.mahfooz.spark.hdfs.config;

public class HdfsConfig {
}
