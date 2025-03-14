package com.mahfooz.spark.mongodb.aggregation;

import com.mahfooz.spark.mongodb.config.MongodbSparkConfig;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import static java.util.Collections.singletonList;

public class SparkMongodbAggregation {

    public static void main(final String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("SparkMongodbAggregation")
                .config("spark.mongodb.input.uri", MongodbSparkConfig.MONGODB_URI)
                .config("spark.mongodb.output.uri", MongodbSparkConfig.MONGODB_URI)
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // Load and analyze data from MongoDB
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        /*Start Example: Use aggregation to filter a RDD***************/
        JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { test : { $gt : 5 } } }")));
        /*End Example**************************************************/

        // Analyze data from MongoDB
        System.out.println(aggregatedRdd.count());
        System.out.println(aggregatedRdd.first().toJson());
        jsc.close();
    }
}
