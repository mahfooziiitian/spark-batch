package com.mahfooz.df.columns.addition;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class AddNewColumn {
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

        df.printSchema();
        Dataset<Row> newDataFrame = df.withColumn("NewInvoiceNo", functions.col("InvoiceNo").plus(1));
        newDataFrame.show();
    }
}
