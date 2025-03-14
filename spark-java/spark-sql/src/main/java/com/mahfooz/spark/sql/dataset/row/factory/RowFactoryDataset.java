package com.mahfooz.spark.sql.dataset.row.factory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RowFactoryDataset {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark")
                .getOrCreate();

        List<Row> imMemory = new ArrayList<Row>();

        imMemory.add(RowFactory.create("WARN", "16 Dec 2020"));
        imMemory.add(RowFactory.create("FATAL", "31 Dec 2020"));
        imMemory.add(RowFactory.create("ERROR", "21 Dec 2020"));
        imMemory.add(RowFactory.create("INFO", "10 Dec 2020"));
        imMemory.add(RowFactory.create("SEVERE", "01 Jan 2021"));

        StructType schema = new StructType(
                new StructField[]{
                        new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
                }
        );

        Dataset<Row> dataset = spark.createDataFrame(imMemory, schema);

        dataset.show();

        spark.stop();

    }
}
