package com.mahfooz.ds.generic.xml;

import com.mahfooz.ds.generic.util.DownloadFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class XmlToDataframeApp {

    public static void main(String[] args ) {
        XmlToDataframeApp app = new XmlToDataframeApp();
        app .start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName( "XML to Dataframe" )
                .master( "local" )
                .getOrCreate();

        String dataHome = System.getenv("DATA_HOME") + "\\spark\\datasource\\xml";
        String downloadUrl = " https://raw.githubusercontent.com/jgperrin/net.jgp.books.spark.ch07/master/data/nasa-patents.xml";
        String filePath = dataHome + "\\nasa-patents.xml";
        if (!(new File(filePath).exists()))
            DownloadFile.downloadFile(downloadUrl, filePath);

        Dataset<Row> df = spark.read().format( "xml" )
            .option( "rowTag" , "row" )
            .load( filePath);

        df.show(5);
        df.printSchema();
    }
}