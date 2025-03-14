package com.mahfooz.delta;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

public class DeltaDs {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .master("local[*]").getOrCreate();

        String dataHome = System.getenv("DATA_HOME");

        Dataset<Row> data = spark.range(0, 5).toDF();
        data.write().format("delta").mode(SaveMode.Overwrite).save(String.format("%s/FileData/Delta/delta-table",dataHome));
    }
}
