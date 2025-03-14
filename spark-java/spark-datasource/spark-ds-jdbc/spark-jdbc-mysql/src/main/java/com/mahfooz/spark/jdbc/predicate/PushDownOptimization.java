package com.mahfooz.spark.jdbc.predicate;

import java.io.IOException;
import java.util.Properties;

import com.mahfooz.spark.jdbc.config.SparkJdbcConfig;
import org.apache.spark.sql.SparkSession;

public class PushDownOptimization {

    public static void main(String[] args) throws IOException {

        Properties properties= SparkJdbcConfig.getProperties("D:\\data\\database\\mysql\\mysql-db-java.properties",
                "bW92ZV9leHRlcm5hbF9hcGk=",
                "PBEWithMD5AndTripleDES");

        String jdbcUrl=properties.getProperty("url");

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("SparkJdbcUsingProperties")
                .getOrCreate();

        spark.read()
                .jdbc(jdbcUrl, "spark.person", properties)
                .explain(true);

        spark.read()
                .jdbc(jdbcUrl, "spark.person", properties)
                .select("id", "name", "email")
                .explain(true);

        // You can push query predicates down to
        // Notice the filter at the top of the physical plan
        spark.read()
                .jdbc(jdbcUrl, "spark.person", properties)
                .select("id", "name", "email")
                .where("name = 'Holmes'")
                .explain(true);
        spark.stop();
    }
}
