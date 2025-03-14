package com.mahfooz.spark.jdbc.encoding;

import com.mahfooz.spark.jdbc.config.SparkJdbcConfig;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Properties;

public class SparkJdbcEncoding {

    public static void main(String[] args) throws IOException {

        Properties properties= SparkJdbcConfig.getProperties("D:\\data\\database\\mysql\\mysql-db-java.properties",
                "bW92ZV9leHRlcm5hbF9hcGk=",
                "PBEWithMD5AndTripleDES");

        String jdbcUrl=properties.getProperty("url");

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("SparkJdbcPartition")
                .getOrCreate();

        spark.read().format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", "encoding")
                .option("encoding", "UTF-8")
                .option("characterEncoding", "UTF-8")
                .option("partitionColumn", "id")
                .option("user", properties.getProperty("user"))
                .option("password", properties.getProperty("password"))
                .option("lowerBound", 10)
                .option("upperBound", 20)
                .option("numPartitions", "5")
                .load()
                .show(100, false);

        spark.stop();
    }
}
