package com.mahfooz.spark.sql.datetime;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DateAndTime {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark")
                .getOrCreate();

        List<Row> imMemory = new ArrayList<Row>();

        imMemory.add(RowFactory.create("WARN", "2020-12-16"));
        imMemory.add(RowFactory.create("FATAL", "2020-12-31"));
        imMemory.add(RowFactory.create("ERROR", "2020-12-21"));
        imMemory.add(RowFactory.create("INFO", "2020-11-23"));
        imMemory.add(RowFactory.create("SEVERE", "2021-01-23"));

        StructType schema = new StructType(
                new StructField[]{
                        new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
                }
        );



        Dataset<Row> dataset = spark.createDataFrame(imMemory, schema);

        dataset.createOrReplaceTempView("logging_table");

        Dataset<Row> results=spark.sql("select level,date_format(datetime,'yyyy') from logging_table");

        results.show();

        spark.stop();

    }
}
