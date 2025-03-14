package com.mahfooz.ds.parquet;

import com.mahfooz.ds.parquet.util.DownloadFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class ParquetToDataframeApp {

    /**
     * main() is your entry point to the application.
     *
     * @param args This is arguments to program
     */
    public static void main(String[] args) {
        ParquetToDataframeApp app = new ParquetToDataframeApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Parquet to Dataframe")
                .master("local[*]")
                .getOrCreate();

        String dataHome = System.getenv("DATA_HOME") + "/FileData/Parquet";
        String downloadUrl = "https://raw.githubusercontent.com/jgperrin/net.jgp.books.spark.ch07/master/data/alltypes_plain.parquet";
        String filePath = dataHome + "/all_types_plain.parquet";
        if (!(new File(filePath).exists()))
            DownloadFile.downloadFile(downloadUrl, filePath);

        // Reads a Parquet file, stores it in a dataframe
        Dataset<Row> df = spark.read()
                .format("parquet")
                .load(filePath);

        // Shows at most 10 rows from the dataframe
        df.show(10);
        df.printSchema();
        System.out.println("The dataframe has " + df.count() + " rows.");
    }
}