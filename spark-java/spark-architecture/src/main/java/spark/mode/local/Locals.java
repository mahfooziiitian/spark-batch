/*

In local mode with a single JVM, the above code will sum the values within the RDD and store it in counter.

<<<<<<< HEAD
 This is because both the RDD and the variable counter are in the same memory space on the driver node.
 
 spark-shell --master=local
 
 
Local Mode (local[*],local,local[2]…etc)

	When you launch spark-shell without control/configuration argument, It will launch in local mode
	
	spark-shell –master local[1]
	
	spark-submit –class com.df.SparkWordCount SparkWC.jar local[1]
=======
This is because both the RDD and the variable counter are in the same memory space on the driver node.
>>>>>>> e4019437105d3d279ee728eba2ce557d4b27a18e

 */
package spark.mode.local;

public class Locals {
}
