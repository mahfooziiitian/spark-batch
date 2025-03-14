package com.mahfooz.ds.generic.csv;

import com.mahfooz.ds.generic.util.DownloadFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class ComplexCsvToDataframeApp {

    public static void main(String[] args ) {
        ComplexCsvToDataframeApp app = new ComplexCsvToDataframeApp();
        app .start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName( "Complex CSV to Dataframe" )
                .master( "local[*]" )
                .getOrCreate();

        String dataHome = System.getenv("DATA_HOME") + "\\spark\\datasource\\csv";
        String downloadUrl = "https://raw.githubusercontent.com/jgperrin/net.jgp.books.spark.ch07/master/data/books.csv";
        String filePath = dataHome + "\\books.csv";
        if (!(new File(filePath).exists()))
            DownloadFile.downloadFile(downloadUrl, filePath);

        Dataset<Row> df = spark .read().format( "csv" )
            .option( "header" , "true" )
            .option( "multiline" , true )
            .option( "sep" , ";" )
            .option( "quote" , "*" )
            .option( "dateFormat" , "M/d/y" )
            .option( "inferSchema" , true )
            .load( filePath );

        System.out.println( "Excerpt of the dataframe content:" );
        df.show(7, 90);
        System.out.println( "Dataframe's schema:" );
        df.printSchema();
    }
}