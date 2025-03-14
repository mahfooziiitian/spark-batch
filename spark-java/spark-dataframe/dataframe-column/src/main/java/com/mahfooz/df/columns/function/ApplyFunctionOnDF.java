package com.mahfooz.df.columns.function;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class ApplyFunctionOnDF {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("RenamingColumn")
                .getOrCreate();

        String dataHome = System.getenv("DATA_HOME");

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(dataHome+"\\FileData\\Csv\\Retail\\all\\online-retail-dataset.csv");

        Dataset<Row> updatedDF = df.withColumn(
                "InvoiceCustomerId",
                functions.concat(
                        functions.col("InvoiceNo"),
                        functions.col("CustomerID")
                )
        );
        updatedDF.show();
    }
}
