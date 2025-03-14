package com.mahfooz.spark.dataset.filter;

import com.mahfooz.spark.dataset.row.DatasetRow;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DatasetFilter {

    public static void main(String[] args) {

        String fileName = args[0];

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName(DatasetRow.class.getSimpleName())
                .getOrCreate();

        // Create an Dataset of Person objects from a text file
        Dataset<Row> dataset = sparkSession.read()
                .option("header", true)
                .csv(fileName);

        dataset.printSchema();

        dataset.filter("count == 20")
                .show();
    }
}
