package com.mahfooz.delta.opitmize;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

public class OptimizeTable {

    public static void main(String[] args) {

        String dataHome = System.getenv("DATA_HOME");
        String warehouse = System.getenv("SPARK_WAREHOUSE");
        String derbyHome = System.getenv("derby.system.home");

        SparkSession spark = SparkSession.builder()
                .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.warehouse.dir", warehouse)
                .config("spark.driver.extraJavaOptions", String.format("-Dderby.system.home='%s'", derbyHome))
                .master("local[*]").getOrCreate();

        Dataset<Row> df = spark.range(0, 1000).toDF();

        // Create table in the metastore using DataFrame's schema and write data to it
        df.write().format("delta").mode(SaveMode.Overwrite).saveAsTable("default.people10m");

        // Create table with path using DataFrame's schema and write data to it
        df.write().format("delta").mode("overwrite").save("/tmp/delta/people10m");

    }
}
