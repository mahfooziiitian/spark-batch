package com.mahfooz.spark.jdbc.savemode;

import com.mahfooz.spark.jdbc.config.SparkJdbcConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Properties;

public class SavingCsvDataToMysql {

    public static void main(String[] args) throws IOException {

        Properties properties= SparkJdbcConfig.getProperties("D:\\data\\database\\mysql\\mysql-db-java.properties",
                "bW92ZV9leHRlcm5hbF9hcGk=",
                "PBEWithMD5AndTripleDES");

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SavingCsvDataToMysql")
                .getOrCreate();

        Dataset<Row> csvDF=spark.read()
                .format("csv")
                .option("sep", ",")
                .option("escape",",")
                .option("header", true)
                .option("inferSchema","true")
                .load("D:\\data\\flight-data\\csv\\flights.csv");

        csvDF.printSchema();

        // Specifying create table column data types on write
        csvDF.write()
                .jdbc(properties.getProperty("url"),
                        "flight",
                        properties);

        spark.stop();
    }
}
