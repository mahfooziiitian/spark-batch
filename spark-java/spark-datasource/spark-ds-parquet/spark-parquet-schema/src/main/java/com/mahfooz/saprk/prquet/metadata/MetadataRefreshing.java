/*
Spark SQL caches Parquet metadata for better performance.
When Hive metastore Parquet table conversion is enabled, metadata of those converted tables are also cached.
If these tables are updated by Hive or other external tools, you need to refresh them manually to ensure consistent metadata.
*/
package com.mahfooz.saprk.prquet.metadata;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MetadataRefreshing {

    public static void main(String[] args) {

            // Create a SparkConf and set the application name
            SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("MetadataRefreshing");

            // Create a SparkSession with the SparkConf
            SparkSession spark = SparkSession.builder()
                    .config(sparkConf)
                    .getOrCreate();

            // Specify the path to the Parquet files
        String dataHome = System.getenv("DATA_HOME");
        String parquetPath = String.format("%s/FileData/Parquet/schema/schema-merge", dataHome);


            // Read Parquet data
            Dataset<Row> data = spark.read().parquet(parquetPath);

            // Show the initial schema and data
            data.printSchema();
            data.show();

            data.createOrReplaceTempView("schema_merge");

            // Simulate a change in the Parquet files (e.g., adding or deleting files)

            // Refresh metadata to reflect the changes
            spark.catalog().refreshTable("schema_merge");

            // Read the data again after refreshing metadata
            Dataset<Row> refreshedData = spark.read().parquet(parquetPath);

            // Show the refreshed schema and data
            refreshedData.printSchema();
            refreshedData.show();

            // Stop the SparkSession when done
            spark.stop();
    }
}
