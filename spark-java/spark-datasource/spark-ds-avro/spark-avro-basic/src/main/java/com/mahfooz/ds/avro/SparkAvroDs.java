/*

Since Spark 2.4 release, Spark SQL provides built-in support for reading and writing Apache Avro data.

*/
package com.mahfooz.ds.avro;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkAvroDs {

    public static void main(String[] arg) {
        String dataHome = System.getenv("DATA_HOME");

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkAvroDs")
                .getOrCreate();

        Dataset<Row> usersDF = spark.read().format("avro")
                .load(String.format("%s/FileData/avro/users.avro", dataHome));

        usersDF.select("name", "favorite_color")
                .write()
                .format("avro")
                .save(String.format("%s/FileData/avro/namesAndFavColors.avro",dataHome));
        
        spark.stop();
    }
}
