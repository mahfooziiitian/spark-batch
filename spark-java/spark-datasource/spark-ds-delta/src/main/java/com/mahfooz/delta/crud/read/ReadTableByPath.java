package com.mahfooz.delta.crud.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadTableByPath {
    public static void main(String[] args) {
        String warehouse = System.getenv("SPARK_WAREHOUSE");

        SparkSession spark = SparkSession.builder()
                .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.warehouse.dir", warehouse)
                .master("local[*]").getOrCreate();

        String inputPath = String.format("%s/DataWarehouse/warehouse/data_table_builder",System.getenv("DATA_HOME"));

        Dataset<Row> df = spark.read().format("delta").load(inputPath);  // create table by path

        df.printSchema();

    }
}
