package com.mahfooz.spark.mongodb.writing;

import com.mahfooz.spark.mongodb.config.MongodbSparkConfig;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;

import static org.sparkproject.guava.primitives.Ints.asList;

public class SparkWritingMongoDbConfig {

    public static void main(final String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", MongodbSparkConfig.MONGODB_URI)
                .config("spark.mongodb.output.uri",MongodbSparkConfig.MONGODB_URI)
                .config("spark.mongodb.output.collection","spark_input")
                .config("spark.mongodb.output.collection","spark_output")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        //Create a custom WriteConfig
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "spark_output");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);

        // Create a RDD of 10 documents
        JavaRDD<Document> sparkDocuments = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
                ((Function<Integer, Document>) i -> Document.parse("{test: " + i + "}"));

        /*Start Example: Save data from RDD to MongoDB*****************/
        MongoSpark.save(sparkDocuments, writeConfig);
        /*End Example**************************************************/

        jsc.close();
    }

}
