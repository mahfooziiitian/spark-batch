package com.mahfooz.ds.avro;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.package$;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;

public class SparkAvroFunction {
    public static void main(String[] args) throws IOException, StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("SparkAvroFunction")
                .getOrCreate();

        // `from_avro` requires Avro schema in JSON string format.
        String jsonFormatSchema = new String(Files.readAllBytes(Paths.get("d:/data/spark/avro/user.avsc")));

        Dataset<Row> df = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                .option("subscribe", "topic1").load();

        // 1. Decode the Avro data into a struct;
        // 2. Filter by column `favorite_color`;
        // 3. Encode the column `name` in Avro format.

        Dataset<Row> output = df.select(package$.MODULE$.from_avro(col("value"), jsonFormatSchema).as("user"))
                .where("user.favorite_color == \"red\"").select(package$.MODULE$.to_avro(col("user.name")).as("value"));

        StreamingQuery query = output.writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                .option("topic", "topic2")
                        .start();

        query.awaitTermination();

    }
}
