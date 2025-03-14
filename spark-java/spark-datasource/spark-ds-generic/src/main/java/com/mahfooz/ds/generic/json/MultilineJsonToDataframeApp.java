package com.mahfooz.ds.generic.json;

import com.mahfooz.ds.generic.util.DownloadFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class MultilineJsonToDataframeApp {

    public static void main(String[] args ) {
        MultilineJsonToDataframeApp app =
                new MultilineJsonToDataframeApp();
        app .start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName( "Multiline JSON to Dataframe" )
                .master( "local" )
                .getOrCreate();

        String dataHome = System.getenv("DATA_HOME") + "\\spark\\datasource\\json";
        String downloadUrl = "https://raw.githubusercontent.com/jgperrin/net.jgp.books.spark.ch07/master/data/countrytravelinfo.json";
        String filePath = dataHome + "\\countrytravelinfo.json";
        if (!(new File(filePath).exists()))
            DownloadFile.downloadFile(downloadUrl, filePath);

        Dataset<Row> df = spark .read()
                .format( "json" )
                .option( "multiline" , true )
            .load( filePath );

        df.show(3);
        df.printSchema();
    }

}
