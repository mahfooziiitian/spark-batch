package com.mahfooz.spark.sql.session;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SparkSessions {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/spark")
                .getOrCreate();

        Dataset<Row> dataset=spark.read()
                .option("header",true)
                .csv(args[0]);

        dataset.createOrReplaceTempView("students");

        //Dataset<Row> results=spark.sql("select * from students where subject='French'");
        Dataset<Row> results=spark.sql("select avg(score) from students where subject='French'");

        results.show();

        spark.stop();
    }
}
