package com.mahfooz.spark.hive.db;

import java.io.File;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DatabaseLocation {
    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();

        SparkSession spark = SparkSession.builder().master("local[*]").appName("DatabaseLocation")
                .config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate();

        spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");
        spark.sql("LOAD DATA LOCAL INPATH '/d/data/spark/hive/kv1.txt' INTO TABLE src");

        // Queries are expressed in HiveQL
        spark.sql("SELECT * FROM src").show();

        // Aggregation queries are also supported.
        spark.sql("SELECT COUNT(*) FROM src").show();

        // The results of SQL queries are themselves DataFrames and support all normal
        // functions.
        Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");

        // The items in DataFrames are of type Row, which lets you to access each column
        // by ordinal.
        Dataset<String> stringsDS = sqlDF.map(
                (MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1),
                 Encoders.STRING());
        stringsDS.show();

        spark.stop();
    }
}
