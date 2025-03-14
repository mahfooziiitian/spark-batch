package com.mahfooz.ds.generic.json;

import com.mahfooz.ds.generic.util.DownloadFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class JsonLinesToDataframeApp {

    public static void main(String[] args) {
        JsonLinesToDataframeApp app =
                new JsonLinesToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("JSON Lines to Dataframe")
                .master("local")
                .getOrCreate();



        String dataHome = System.getenv("DATA_HOME") + "\\spark\\datasource\\json";
        String downloadUrl = "https://raw.githubusercontent.com/jgperrin/net.jgp.books.spark.ch07/master/data/durham-nc-foreclosure-2006-2016.json";
        String filePath = dataHome + "\\durham-nc-foreclosure-2006-2016.json";
        if (!(new File(filePath).exists()))
            DownloadFile.downloadFile(downloadUrl, filePath);

        Dataset<Row> df = spark.read().format("json")
                .load(filePath);

        df.show(5, 13);
        df.printSchema();
    }
}