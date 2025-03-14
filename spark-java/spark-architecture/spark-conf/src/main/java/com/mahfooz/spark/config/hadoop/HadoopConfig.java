/*
If you plan to read and write from HDFS using Spark, there are two Hadoop configuration files that should be included on Spark’s
classpath:

    hdfs-site.xml, which provides default behaviors for the HDFS client.
    core-site.xml, which sets the default filesystem name.

The location of these configuration files varies across Hadoop versions, but a common location is inside of /etc/hadoop/conf.
Some tools create configurations on-the-fly, but offer a mechanism to download copies of them.
To make these files visible to Spark, set HADOOP_CONF_DIR in $SPARK_HOME/conf/spark-env.sh to a location containing the configuration
files.

Multiple running applications might require different Hadoop/Hive client side configurations.
You can copy and modify hdfs-site.xml, core-site.xml, yarn-site.xml, hive-site.xml in Spark’s classpath for each application.
In a Spark cluster running on YARN, these configuration files are set cluster-wide, and cannot safely be changed by the application.
The better choice is to use spark hadoop properties in the form of spark.hadoop.*.
They can be considered as same as normal spark properties which can be set in $SPARK_HOME/conf/spark-defaults.conf.

 */
package com.mahfooz.spark.config.hadoop;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class HadoopConfig {

    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf().set("spark.hadoop.abc.def","xyz")
                .setMaster("local[*]")
                .setAppName(HadoopConfig.class.getSimpleName());
        SparkContext sparkContext=new SparkContext(sparkConf);

    }

}
