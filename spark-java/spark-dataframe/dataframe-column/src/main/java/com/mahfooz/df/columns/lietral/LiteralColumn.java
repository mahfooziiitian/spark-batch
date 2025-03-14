package com.mahfooz.df.columns.lietral;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class LiteralColumn {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SubSelectColumn")
                .getOrCreate();

        String dataHome = System.getenv("DATA_HOME");

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(dataHome+"\\FileData\\Csv\\Retail\\all\\online-retail-dataset.csv");

        // Adding a new column with a constant literal value
        Dataset<Row> newDataFrame = df.withColumn(
                "Factors",
                functions.lit(1.5)
        );

        newDataFrame.show();

    }
}
