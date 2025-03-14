package com.mahfooz.delta.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class ReadStreamChanges {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        String dataHome = System.getenv("DATA_HOME");

        SparkSession spark = SparkSession.builder()
                .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .master("local[*]").getOrCreate();

        String streamDataLocation = String.format("%s/FileData/Delta/streaming/delta-stream",dataHome);

        StreamingQuery stream = spark.readStream()
                .format("delta")
                .load(streamDataLocation)
                .writeStream()
                .format("console")
                .start();

        stream.awaitTermination();
    }
}
