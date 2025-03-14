package com.mahfooz.spark.jdbc.savemode;

import java.io.IOException;
import java.util.Properties;

import com.mahfooz.spark.jdbc.config.SparkJdbcConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreatingTableWhileSaving {

    public static void main(String[] args) throws IOException {

        Properties properties= SparkJdbcConfig.getProperties("D:\\data\\database\\mysql\\mysql-db-java.properties",
                "bW92ZV9leHRlcm5hbF9hcGk=",
                "PBEWithMD5AndTripleDES");

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("CreatingTableWhileSaving")
                .getOrCreate();


        Dataset<Row> jdbcDF = spark.read()
                .jdbc(properties.getProperty("url"),
                        "emp_tab",
                        properties);

        // Specifying create table column data types on write
        jdbcDF.write()
                .option("createTableColumnTypes", "emp_id int, name VARCHAR(30)" +
                        ", dept_id int")
                .jdbc(properties.getProperty("url"),
                        "emp_saving",
                        properties);

        spark.stop();
    }
}
