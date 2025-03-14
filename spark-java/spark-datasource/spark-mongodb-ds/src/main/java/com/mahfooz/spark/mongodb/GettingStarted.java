package com.mahfooz.spark.mongodb;

import com.mahfooz.spark.mongodb.config.MongodbSparkConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class GettingStarted {

        public static void main(final String[] args) throws InterruptedException {
            /* Create the SparkSession.
             * If config arguments are passed from the command line using --conf,
             * parse args for the values to set.
             */
            SparkSession spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("MongoSparkConnectorIntro")
                    .config("spark.mongodb.input.uri", MongodbSparkConfig.MONGODB_URI)
                    .getOrCreate();
            // Create a JavaSparkContext using the SparkSession's SparkContext object
            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
            // More application logic would go here...
            jsc.close();
        }
}
