package com.mahfooz.spark.orc.implementation.natives;

import com.mahfooz.spark.orc.util.DownloadFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class OrcToDataframeNativeApp {

    public static void main(String[] args ) {
        OrcToDataframeNativeApp app = new OrcToDataframeNativeApp();
        app .start();
    }
    private void start() {
        String sparkWarehouse = System.getenv("SPARK_WAREHOUSE");
        SparkSession spark = SparkSession.builder()
                .appName( "ORC to Dataframe" )
                .config("spark.sql.warehouse.dir", sparkWarehouse)
                .master ("local[*]" )
                .getOrCreate();

        String dataHome = System.getenv("DATA_HOME") + "\\FileData\\Orc";
        String downloadUrl = "https://raw.githubusercontent.com/jgperrin/net.jgp.books.spark.ch07/master/data/demo-11-zlib.orc";
        String filePath = dataHome + "\\demo-11-zlib.orc";
        if (!(new File(filePath).exists()))
            DownloadFile.downloadFile(downloadUrl, filePath);

        Dataset<Row> df = spark .read()
                .format( "orc" )
            .load( filePath );

        df .show(10);
        df .printSchema();
        System.out.println( "The dataframe has " + df .count() + " rows." );
    }
}
