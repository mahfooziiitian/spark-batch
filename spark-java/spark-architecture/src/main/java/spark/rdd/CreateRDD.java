/*

Ways to create Spark RDD

Basically, there are 3 ways to create Spark RDDs

i. Parallelized collections

	By invoking parallelize method in the driver program, we can create parallelized collections.
	
	val stringList = Array("Spark is awesome","Spark is cool")
	val stringRDD = sc.parallelize(stringList)

ii. External datasets

	One can create Spark RDDs, by calling a textFile method.
	Hence, this method takes URL of the file and reads it as a collection of lines.
	
	The second way to create an RDD is to read a dataset from a storage system, which can be a local computer file system, HDFS, 
	Cassandra, Amazon S3, and so on
	
	val fileRDD = spark.sparkContext.textFile("/tmp/data.txt")
	The first argument of the textFile method is an URI that points to a path or a file on the local machine or to a remote storage system.
	When it starts with an hdfs:// prefix, it points to a path or a file that resides on HDFS, and when it starts with an s3n:// prefix, 
		then it points to a path or a file that resides on AWS S3. 
	If a URI points to a directory, then the textFile method will read all the files in that directory.

iii. Existing RDDs

	Moreover, we can create new RDD in spark, by applying transformation operation on existing RDDs.

Using the Shell

	 ./bin/spark-shell --master local[4]

	 /bin/spark-shell --master local[4] --jars code.jar

	 ./bin/spark-shell --master local[4] --packages "org.example:example:0.1"
 */
package spark.rdd;

import java.util.List;
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;

//Finally, you need to import some Spark classes into your program
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;

public class CreateRDD {

    private static String appName = "";

    private static String master = "";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        List <Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD <Integer> distData = sc.parallelize(data);


        //Passing Functions to Spark
        JavaRDD <String> lines = sc.textFile("data.txt");

        JavaRDD <Integer> lineLengths = lines.map(new Function <String, Integer>() {

            @Override
            public Integer call(String s) {
                return s.length();
            }
        });


        int totalLength = lineLengths.reduce(new Function2 <Integer, Integer, Integer>() {

            @Override
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });

    }

}

class GetLength implements Function <String, Integer> {

    @Override
    public Integer call(String s) {
        return s.length();
    }
}

class Sum implements Function2<Integer, Integer, Integer> {

    @Override
    public Integer call(Integer a, Integer b) {
        return a + b;
    }
}

