package com.mahfooz.spark.orc.schema;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * When true, the ORC data source merges schemas collected from all data files, otherwise the schema is picked from a random data file.
 */
public class SchemaMergingDatasourceLevel {

    public static void main(String[] args) {
        // Configure Spark
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SchemaMergingDatasourceLevel");
        try(SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate()) {

            // Specify the input ORC file path
            String inputPath = String.format("%s/FileData/Orc/schema/orc-schema-merge",System.getenv("DATA_HOME"));

            // Read ORC files into a DataFrame
            Dataset<Row> orcDataFrame = spark.read()
                    .option("mergeSchema", true)
                    .orc(inputPath);

            // Perform operations on the DataFrame as needed
            orcDataFrame.show();
        }
    }
}
