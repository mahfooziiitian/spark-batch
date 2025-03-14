/*

SparkSession uses builder design pattern to create an instance.

This pattern will reuse a Spark context if it exists, otherwise, creates a new Spark context.

val sparkSession = SparkSession.builder.
  master("local")
  .appName("Spark session in Fresco")
  .getOrCreate()

The value local for the master is used as a default value to launch the application in test environment locally.

SparkSession with Hive

	val sparkSession = SparkSession.builder.
  master("local")
  .appName("Spark session with hive in Fresco")
  .enableHiveSupport()
  .getOrCreate()

  
*/
package spark.session;

public class SparkSession {
	
	
}