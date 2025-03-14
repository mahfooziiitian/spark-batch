package com.mahfooz.spark.jdbc.prop;

import java.io.IOException;
import java.util.Properties;

import com.mahfooz.spark.jdbc.config.SparkJdbcConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkJdbcUsingProperties {

    public static void main(String[] args) throws IOException {

        Properties properties= SparkJdbcConfig.getProperties("D:\\data\\database\\mysql\\mysql-db-java.properties",
                "bW92ZV9leHRlcm5hbF9hcGk=",
                "PBEWithMD5AndTripleDES");

        String jdbcUrl=properties.getProperty("url");
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("SparkJdbcUsingProperties")
                .getOrCreate();

        Dataset<Row> jdbcDF = spark.read()
                .jdbc(jdbcUrl,
                        "spark.person",
                        properties);

        // Saving data to a JDBC source
        jdbcDF.write()
                .jdbc(jdbcUrl,
                "spark.person_prop",
                properties);

        spark.stop();
    }
}
