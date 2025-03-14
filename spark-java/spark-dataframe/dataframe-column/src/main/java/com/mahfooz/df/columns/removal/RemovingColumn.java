package com.mahfooz.df.columns.removal;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RemovingColumn {
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

        Dataset<Row> withoutColumnDF = df.drop("Description");
        withoutColumnDF.show();
    }
}
