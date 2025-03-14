/*
Make sure that the database has an index on the partitioning column.

*/
package com.mahfooz.spark.jdbc.partition;

import com.mahfooz.spark.jdbc.config.SparkJdbcConfig;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Properties;

public class SparkJdbcPartition {

    public static void main(String[] args) throws IOException {

        Properties properties= SparkJdbcConfig.getProperties("D:\\data\\database\\mysql\\mysql-db-java.properties",
                "bW92ZV9leHRlcm5hbF9hcGk=",
                "PBEWithMD5AndTripleDES");

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("SparkJdbcPartition")
                .getOrCreate();

        spark.read()
                .format("jdbc")
                .option("url", properties.getProperty("url"))
                .option("dbtable", "emp_tab")
                .option("partitionColumn", "emp_id")
                .option("user", properties.getProperty("user"))
                .option("password", properties.getProperty("password"))
                .option("lowerBound", 1)
                .option("upperBound", 20)
                .option("numPartitions", "5")
                .load()
                .show();

        spark.stop();
    }
}
