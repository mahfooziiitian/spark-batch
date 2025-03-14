package com.mahfooz.ds.generic.text;

import com.mahfooz.ds.generic.util.DownloadFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class TextToDataframeApp {

    public static void main(String[] args ) {
        TextToDataframeApp app = new TextToDataframeApp();
        app .start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName( "Text to Dataframe" )
                .master( "local" )
                .getOrCreate();

        String dataHome = System.getenv("DATA_HOME") + "\\spark\\datasource\\text";
        String downloadUrl = "https://raw.githubusercontent.com/jgperrin/net.jgp.books.spark.ch07/master/data/romeo-juliet-pg1777.txt";
        String filePath = dataHome + "\\durham-nc-foreclosure-2006-2016.json";
        if (!(new File(filePath).exists()))
            DownloadFile.downloadFile(downloadUrl, filePath);


        Dataset<Row> df = spark .read().format( "text" )
            .load( filePath );

        df .show(10);
        df .printSchema();
    }
}