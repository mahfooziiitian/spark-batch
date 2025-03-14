package com.mahfooz.delta.streaming;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.streaming.StreamingQueryException;


public class WritingStream {

    public static void main(String[] args) throws StreamingQueryException {

        String dataHome = System.getenv("DATA_HOME");

        SparkSession spark = SparkSession.builder()
                .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .master("local[*]").getOrCreate();

        Dataset<Row> streamingDf = spark.readStream().format("rate").load();

        String checkpointLocation = String.format("%s/Processing/Batch/Spark/checkpoint/delta-stream",dataHome);
        String streamDataLocation = String.format("%s/FileData/Delta/streaming/delta-stream",dataHome);

        StreamingQuery stream = streamingDf.selectExpr("value as id")
                .writeStream()
                .format("delta")
                .option("checkpointLocation", checkpointLocation)
                .start(streamDataLocation);

        stream.awaitTermination();

    }
}
