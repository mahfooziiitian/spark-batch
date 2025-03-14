/*

Parquet is a columnar format that is supported by many other data processing systems.
Spark SQL provides support for both reading and writing Parquet files that automatically preserves the schema of the original data.
When writing Parquet files, all columns are automatically converted to be nullable for compatibility reasons.

*/
package com.mahfooz.ds.parquet;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkParquetDs {

    public static void main(String[] arg) {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("SparkAvroDs").getOrCreate();

        Dataset<Row> peopleDF = spark.read().json("d:/data/spark/json/people.json");

        // DataFrames can be saved as Parquet files, maintaining the schema information
        peopleDF.write().parquet("d:/data/spark/parquet/people.parquet");

        // Read in the Parquet file created above.
        // Parquet files are self-describing so the schema is preserved
        // The result of loading a parquet file is also a DataFrame
        Dataset<Row> parquetFileDF = spark.read().parquet("d:/data/spark/parquet/people.parquet");

        // Parquet files can also be used to create a temporary view and then used in SQL statements
        parquetFileDF.createOrReplaceTempView("parquetFile");
        Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19");
        Dataset<String> namesDS = namesDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        namesDS.show();
        spark.stop();
    }
}
