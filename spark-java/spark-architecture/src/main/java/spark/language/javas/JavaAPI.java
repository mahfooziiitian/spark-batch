/*

Initializing Spark in Java

import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaSparkContext;

SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");

JavaSparkContext sc = new JavaSparkContext(conf);

 */
package spark.language.javas;

public class JavaAPI {
	
	public static void main(String[] args) {
		
	    //String logFile = "YOUR_SPARK_HOME/README.md"; // Should be some file on your system
	    ///SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
	   // Dataset<String> logData = spark.read().textFile(logFile).cache();

	    //long numAs = logData.filter(s -> s.contains("a")).count();
	    //long numBs = logData.filter(s -> s.contains("b")).count();

	    //System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

	    //spark.stop();
	  }

}
