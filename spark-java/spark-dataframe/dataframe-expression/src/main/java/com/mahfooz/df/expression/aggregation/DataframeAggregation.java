package com.mahfooz.df.expression.aggregation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class DataframeAggregation {

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

        // Aggregating data based on a column
        Dataset<Row> aggregatedDF = df
                .groupBy("Country")
                .agg(functions.sum("UnitPrice").as("PriceSum"));

        aggregatedDF.show();

    }
}
