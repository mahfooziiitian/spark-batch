package com.mahfooz.delta.timetravel;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

public class TimeTravelVersion {

    public static void main(String[] args) {

        String dataHome = System.getenv("DATA_HOME");

        SparkSession spark = SparkSession.builder()
                .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .master("local[*]").getOrCreate();

        Dataset<Row> df = spark.read()
                .format("delta")
                .option("versionAsOf", 0)
                .load(String.format("%s/FileData/Delta/crud/merge-table",dataHome));

        df.show();

        df = spark.read()
                .format("delta")
                .option("versionAsOf", 1)
                .load(String.format("%s/FileData/Delta/crud/merge-table",dataHome));

        df.show();
    }
}
