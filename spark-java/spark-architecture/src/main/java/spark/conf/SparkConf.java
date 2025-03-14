/*

SparkConf stores configuration parameters for a Spark application.

These configuration parameters can be properties of the Spark driver application or utilized by Spark to allot resources on the cluster, 
like memory size and cores.

SparkConf object can be created with new SparkConf() and permits you to configure standard properties and arbitrary key-value pairs via the 
set() method.


val conf = new SparkConf()
             .setMaster("local[4]")
             .setAppName("FirstSparkApp")

val sc = new SparkContext(conf)



 */
package spark.conf;

public class SparkConf {

}
