/*

The sql function on a SparkSession enables applications to run SQL queries programmatically and returns the 
result as a Dataset<Row>.

*/
package com.mahfooz.sql.view.temp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TemporaryView {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[*]").appName("TemporaryView").getOrCreate();

        Dataset<Row> df = spark.read().json("d:/data/spark/json/people.json");

        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();

        spark.close();
    }
}
