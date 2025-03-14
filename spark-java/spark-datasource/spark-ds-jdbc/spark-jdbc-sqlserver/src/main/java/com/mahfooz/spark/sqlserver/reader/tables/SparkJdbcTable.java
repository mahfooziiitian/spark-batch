package com.mahfooz.spark.sqlserver.reader.tables;

import com.mahfooz.spark.sqlserver.util.JasyptEncryptDecrypt;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkJdbcTable {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("SparkJdbcTable")
                .getOrCreate();

        // Loading data from a JDBC source
        Config config = ConfigFactory.load();
        String secretKey = System.getenv("JASYPT_SECRET_KEY");
        String decryptedPassword = JasyptEncryptDecrypt.decryptPassword(
                config.getString("db.user.password"),
                secretKey
        );

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", config.getString("db.url"))
                .option("dbtable", "dbo.Configuration")
                .option("user", config.getString("db.user.id"))
                .option("password", decryptedPassword)
                .load();

        jdbcDF.show();
        // Saving data to a JDBC source
        /*jdbcDF.write().format("jdbc")
               .option("url", config.getString("db.url"))
                .option("dbtable", "dbo.Configuration")
                .option("user", config.getString("db.user.id"))
                .option("password", decryptedPassword)
                .save();*/
        spark.stop();
    }
}
