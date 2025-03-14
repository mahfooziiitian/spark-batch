package com.mahfooz.df.expression.strings;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class StringExpression {

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

        // Concatenating two columns and creating a new column
        Dataset<Row> concatenatedDF = df.withColumn(
                "CustomerInvoice",
                functions.concat_ws(
                        " ",
                        functions.col("InvoiceNo"),
                        functions.col("CustomerID")
                )
        );
        concatenatedDF.show();

    }
}
