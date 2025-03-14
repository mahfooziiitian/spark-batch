/*

Spark SQL works to access structured and semi-structured information.

It also enables powerful, interactive, analytical application across both streaming and historical data.

Spark SQL is Spark module for structured data processing. Thus, it acts as a distributed SQL query engine.

There are several ways to interact with Spark SQL including SQL, the DataFrames API and the Datasets API.

One use of Spark SQL is to execute SQL queries written using either a basic SQL syntax or HiveQL.

Spark SQL can also be used to read data from an existing Hive installation.

When running SQL from within another programming language the results will be returned as a DataFrame.


 */
package spark.component.sql;

//Finally, you need to import some Spark classes into your program
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.SparkConf;

public class SparkSQL {

    //The appName parameter is a name for your application to show on the cluster UI.
    private static String appName="";

    //master is a Spark, Mesos or YARN cluster URL, or a special “local” string to run in local mode.
    //In practice, when running on a cluster, you will not want to hardcode master in the program, but rather launch the application with spark-submit and receive it there
    private static String master="";

    public static void main(String [] args ) {

        //Initializing Spark
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);

        //The first thing a Spark program must do is to create a JavaSparkContext object, which tells Spark how to access a cluster.
        JavaSparkContext sc = null; // An existing JavaSparkContext.
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
    }
}
