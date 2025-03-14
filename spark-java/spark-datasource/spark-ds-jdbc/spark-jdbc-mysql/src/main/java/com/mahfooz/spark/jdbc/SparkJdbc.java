package com.mahfooz.spark.jdbc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkJdbc {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[*]").appName("SaprkJdbc").getOrCreate();

        // Note: JDBC loading and saving can be achieved via either the load/save or
        // jdbc methods
        // Loading data from a JDBC source
        Dataset<Row> jdbcDF = spark.read().format("jdbc").option("url", "jdbc:mysql://localhost:31306/spark")
                .option("dbtable", "spark.person").option("driver", "com.mysql.cj.jdbc.Driver").option("user", "root")
                .option("password", "mysql").load();

        // Saving data to a JDBC source
        jdbcDF.write().format("jdbc").option("url", "jdbc:mysql://localhost:31306/spark")
                .option("dbtable", "spark.person_new").option("user", "root")
                .option("driver", "com.mysql.cj.jdbc.Driver").option("password", "mysql").save();

        spark.stop();
    }
}
