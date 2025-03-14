/*

You can push down an entire query to the database and return just the result.
 You can use anything that is valid in a SQL query FROM clause.
*/
package com.mahfooz.spark.jdbc.predicate;

import java.io.IOException;
import java.util.Properties;

import com.mahfooz.spark.jdbc.config.SparkJdbcConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PushDownQuery {

    public static void main(String[] args) throws IOException {

        Properties properties= SparkJdbcConfig.getProperties("D:\\data\\database\\mysql\\mysql-db-java.properties",
                "bW92ZV9leHRlcm5hbF9hcGk=",
                "PBEWithMD5AndTripleDES");

        String jdbcUrl = properties.getProperty("url");
        String pushDownQuery = "(select * from emp_tab where emp_id >2) emp";

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", properties.getProperty("user"));
        connectionProperties.put("password", properties.getProperty("password"));

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("PushDownQuery")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .jdbc(jdbcUrl,
                        pushDownQuery,
                        connectionProperties);

        df.show();

        spark.stop();
    }
}
