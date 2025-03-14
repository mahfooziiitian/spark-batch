package com.mahfooz.delta.crud.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadTableFromMetastore {

    public static void main(String[] args) {
        String warehouse = System.getenv("SPARK_WAREHOUSE");
        String derbyHome = System.getenv("derby.system.home");

        SparkSession spark = SparkSession.builder()
                .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.warehouse.dir", warehouse)
                .config("javax.jdo.option.ConnectionURL", String.format("jdbc:derby:%s/metastore_db;create=true", derbyHome.replace("\\","/")))
                .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
                .enableHiveSupport()
                .config("spark.driver.extraJavaOptions", String.format("-Dderby.system.home='%s'", derbyHome))
                .master("local[*]").getOrCreate();

        // query table in the metastore
        Dataset<Row> df = spark.table("default.people10m");

        df.printSchema();

    }
}
