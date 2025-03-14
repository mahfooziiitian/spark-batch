package com.mahfooz.spark.uaf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkUaf {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[*]").appName("SparkUaf").getOrCreate();

        // Register the function to access it
        spark.udf().register("myAverage", new MyAverage());

        Dataset<Row> df = spark.read().json("d:/data/spark/json/employees.json");
        df.createOrReplaceTempView("employees");
        df.show();

        Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
        result.show();

    }
}
