package com.mahfooz.spark.dsl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SparkDsl {

    public static void main(String[] args) {

        String fileName = args[0];

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName(SparkDsl.class.getSimpleName())
                .getOrCreate();

        Dataset<Row> dataset= sparkSession.read()
                .option("header",true)
                .csv(fileName);

        dataset=dataset.select(col("level"),
                date_format(col("datetime"),"MMM").alias("month"),
                date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));

        List<Object> columns=new ArrayList<Object>();
        columns.add("March");


        dataset=dataset.groupBy("level").pivot("month",columns).count();

        dataset.show(100);

        sparkSession.stop();
    }
}
