package com.mahfooz.spark.orc.vectorized;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class VectorizedOrcReader {
    public static void main(String[] args) {

        String dataHome = System.getenv("DATA_HOME");

        try(SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate()) {
            // Read Avro data into a DataFrame
            String inputPath = String.format("%s/FileData/Orc/person.orc", dataHome);
            Dataset<Row> df = spark.read().format("orc").load(inputPath);
            // Show the content of the DataFrame
            df.show();
        }
    }
}
