package com.mahfooz.spark.mongodb.reading;

import com.mahfooz.spark.mongodb.config.MongodbSparkConfig;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

public class ReadFromMongoDB {

    public static void main(final String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", MongodbSparkConfig.MONGODB_URI)
                .config("spark.mongodb.output.uri", MongodbSparkConfig.MONGODB_URI)
                .config("spark.mongodb.input.collection","spark")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        /*Start Example: Read data from MongoDB************************/
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        /*End Example**************************************************/

        // Analyze data from MongoDB
        System.out.println(rdd.count());
        System.out.println(rdd.first().toJson());
        jsc.close();
    }
}
