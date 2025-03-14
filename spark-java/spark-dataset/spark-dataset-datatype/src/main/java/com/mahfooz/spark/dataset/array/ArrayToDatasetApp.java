package com.mahfooz.spark.dataset.array;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class ArrayToDatasetApp {

    public static void main(String[] args) {
        ArrayToDatasetApp app = new ArrayToDatasetApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession. builder ()
                .appName( "Array to Dataset<String>" )
                .master( "local[*]" )
                .getOrCreate();

        String[] stringList =
                new String[] { "Jean" , "Liz" , "Pierre" , "Lauric" };
        List<String> data = Arrays. asList ( stringList );
        Dataset<String> ds = spark.createDataset(data , Encoders.STRING());
        ds.show();
        ds.printSchema();

        Dataset<Row> df = ds.toDF();
        df.show();
        df.printSchema();
    }


}
