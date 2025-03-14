package com.mahfooz.spark.jdbc.schema;

import com.mahfooz.spark.jdbc.config.SparkJdbcConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Properties;

public class SparkJdbcSchemaTable {

    public static void main(String[] args) throws IOException {

        Properties properties= SparkJdbcConfig.getProperties("D:\\data\\database\\mysql\\mysql-db-java.properties",
                "bW92ZV9leHRlcm5hbF9hcGk=",
                "PBEWithMD5AndTripleDES");

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("SparkJdbcSchemaTable")
                .getOrCreate();

        // JDBC loading and saving can be achieved via either the load/save or
        // jdbc methods. Loading data from a JDBC source
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", properties.getProperty("url"))
                .option("dbtable", "emp")
                .option("driver", properties.getProperty("driver"))
                .option("user", properties.getProperty("user"))
                .option("password", properties.getProperty("password"))
                .load();

        jdbcDF.printSchema();

        jdbcDF.show();

        spark.stop();
    }
}
