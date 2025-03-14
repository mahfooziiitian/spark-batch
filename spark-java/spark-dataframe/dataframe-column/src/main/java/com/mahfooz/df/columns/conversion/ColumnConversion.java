package com.mahfooz.df.columns.conversion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class ColumnConversion {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("ColumnConversion")
                .getOrCreate();

        String dataHome = System.getenv("DATA_HOME");

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(dataHome+"\\FileData\\Csv\\Retail\\all\\online-retail-dataset.csv");

        //Converting String to Integer
        Dataset<Row> convertedDF = df.withColumn(
                "InvoiceNumber",
                functions.col("InvoiceNo").cast("integer")
        );
        convertedDF.show();

        //Converting String to Date
        //Converting Unix Timestamp to Date
        Dataset<Row> convertedDateDF = df.withColumn(
                "InvoiceDate",
                functions.to_timestamp(
                        functions.col("InvoiceDate"),
                        "dd/MM/yyyy k:mm"
                )
        );
        convertedDateDF.show();

        //Converting String to Double:


    }
}
