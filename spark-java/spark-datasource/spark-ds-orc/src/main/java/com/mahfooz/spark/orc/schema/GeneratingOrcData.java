package com.mahfooz.spark.orc.schema;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class GeneratingOrcData {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();

        List<Row> data1 = Arrays.asList(
                RowFactory.create("Category A", 100),
                RowFactory.create("Category B", 200),
                RowFactory.create("Category C", 300));

        List<Row> data2 = Arrays.asList(
                RowFactory.create("Category D",
                        100,
                        "D - 1"),
                RowFactory.create("Category E",
                        200,
                        "E - 1"
                ));

        StructType schema1 = new StructType()
                .add("category", DataTypes.StringType)
                .add("amount", DataTypes.IntegerType);

        String outputPath = String.format("%s/FileData/Orc/schema/orc-schema-merge",System.getenv("DATA_HOME"));

        spark.createDataFrame(data1, schema1)
                .write().format("orc")
                .mode("overwrite")
                .save(outputPath);

        StructType schema2 = new StructType()
                .add("category", DataTypes.StringType)
                .add("amount", DataTypes.IntegerType)
                .add("sub_category", DataTypes.StringType);

        spark.createDataFrame(data2, schema2)
                .write().format("orc")
                .mode("append")
                .save(outputPath);
    }
}
