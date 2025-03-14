package spark.closure;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.List;

public class Closures {

    private static String appName = "";

    private static String master = "";

    private static List<Integer> data=null;

    public static void main(String [] args ) {

        int counter = 0;

        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(data);

        // Wrong: Don't do this!!
        //rdd.foreach(x -> counter += x);

        System.out.println("Counter value: " + counter);
    }
}
