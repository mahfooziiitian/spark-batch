/*

val sc = new SparkContext(new SparkConf())

spark-submit --name "My app" --master local[4] --conf spark.eventLog.enabled=false
  --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" myApp.jar

 */
package com.mahfooz.spark.config.dynamic

class DynamicConfig {

}
