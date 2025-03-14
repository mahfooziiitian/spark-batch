package com.mahfooz.dataset.untyped;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlDataset {
    public static void main(String[] args) {

        String jsonFilePath = "D:/data/spark/json/people.json";
        SparkSession spark = SparkSession.builder().appName("UntypedDataset").master("local[*]").getOrCreate();

        Dataset<Row> df = spark.read().json(jsonFilePath);

        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();
    }
}
