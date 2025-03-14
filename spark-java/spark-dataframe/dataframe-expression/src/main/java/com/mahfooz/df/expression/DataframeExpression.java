package com.mahfooz.df.expression;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class DataframeExpression {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("DataframeExpression")
                .master("local[*]")
                .getOrCreate();
        String dataHome = System.getenv("DATA_HOME");

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(dataHome+"\\FileData\\Csv\\Retail\\all\\online-retail-dataset.csv");

        // Creating a new column by adding two existing columns
        Dataset<Row> newDataFrame = df.withColumn(
                "Price",
                functions.col("Quantity").multiply(functions.col("UnitPrice")
                )
        );
        newDataFrame.show();
    }
}
