package com.mahfooz.ds.generic.loadsave.format;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkJsonFormat {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[*]").appName("SparkJsonFormat").getOrCreate();

        Dataset<Row> peopleDF = spark.read().format("json").load("d:/data/spark/json/people.json");
        peopleDF.select("name", "age").write().format("json").save("d:/data/spark/json/namesAndAges.json");

        spark.stop();
    }
}
