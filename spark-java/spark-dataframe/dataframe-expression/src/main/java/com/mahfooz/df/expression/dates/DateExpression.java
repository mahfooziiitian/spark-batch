package com.mahfooz.df.expression.dates;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class DateExpression {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("DataframeExpression")
                .master("local[*]")
                .getOrCreate();

        String dataHome = System.getenv("DATA_HOME");

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(dataHome + "\\FileData\\Csv\\Retail\\all\\online-retail-dataset.csv");

        // Extracting year and month from a date column
        Dataset<Row> modifiedDateDF = df.withColumn(
                        "InvoiceTimestamp",
                        functions.to_timestamp(functions.col("InvoiceDate"), "dd/MM/yyyy k:mm")
                )
                .withColumn(
                        "year",
                        functions.year(functions.col("InvoiceTimestamp"))
                )
                .withColumn(
                        "month",
                        functions.month(functions.col("InvoiceTimestamp"))
                );
        modifiedDateDF.show();

    }
}
