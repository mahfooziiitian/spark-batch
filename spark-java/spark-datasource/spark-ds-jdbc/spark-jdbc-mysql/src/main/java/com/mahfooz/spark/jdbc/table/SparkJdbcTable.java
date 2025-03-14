package com.mahfooz.spark.jdbc.table;

import java.io.IOException;
import java.util.Properties;

import com.mahfooz.spark.jdbc.config.SparkJdbcConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import java.util.Base64;

public class SparkJdbcTable {

    public static void main(String[] args) throws IOException {

        Properties properties= SparkJdbcConfig.getProperties("D:\\data\\database\\mysql\\mysql-db-java.properties",
                "bW92ZV9leHRlcm5hbF9hcGk=",
                "PBEWithMD5AndTripleDES");

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("SparkJdbcTable")
                .getOrCreate();

        String jdbcUrl= properties.getProperty("url");

        Dataset<Row> jdbcDF = spark.read().jdbc(
                jdbcUrl,
                "emp",
                properties);

        jdbcDF.createOrReplaceTempView("emp");

        spark.table("emp")
                .withColumnRenamed("emp_name", "name")
                .write().mode(SaveMode.Append)
                .jdbc(jdbcUrl,
                        "emp_tab",
                        properties);

        spark.stop();
    }
}
