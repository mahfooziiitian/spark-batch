package com.mahfooz.df.expression.conditional;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class ConditionalExpression {

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

        // Applying conditional expression and creating a new column
        Dataset<Row> conditionalDF = df.withColumn(
                "CountryShort",
                functions.when(
                        functions.col("Country").equalTo("United Kingdom"),
                        "UK"
                ).otherwise("OT")
        );
        conditionalDF.show(100);
    }
}
