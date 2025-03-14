package com.mahfooz.delta.ddl;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.SparkSession;

public class DataTableBuilderDemo {

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

        // Create table in the metastore
        DeltaTable.createOrReplace(spark)
                .tableName("default.data_table_builder")
                .addColumn("id", "INT")
                .addColumn("firstName", "STRING")
                .addColumn("middleName", "STRING")
                .addColumn(DeltaTable.columnBuilder("lastName")
                                .dataType("STRING")
                                .comment("surname")
                                .build()
                )
                .addColumn("gender", "STRING")
                .addColumn("birthDate", "TIMESTAMP")
                .addColumn("ssn", "STRING")
                .addColumn("salary", "INT")
                .execute();
    }
}
