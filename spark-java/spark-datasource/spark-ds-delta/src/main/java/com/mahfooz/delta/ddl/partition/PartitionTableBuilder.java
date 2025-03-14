package com.mahfooz.delta.ddl.partition;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.SparkSession;

public class PartitionTableBuilder {

    public static void main(String[] args) {

        String warehouse = System.getenv("SPARK_WAREHOUSE");
        String derbyHome = System.getenv("derby.system.home");

        SparkSession spark = SparkSession.builder()
                .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.warehouse.dir", warehouse)
                .config("javax.jdo.option.ConnectionURL", String.format("jdbc:derby:%s/metastore_db;create=true", derbyHome.replace("\\","/")))
                .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
                .config("spark.driver.extraJavaOptions", String.format("-Dderby.system.home='%s'", derbyHome))
                .enableHiveSupport()
                .master("local[*]").getOrCreate();

        System.out.println(spark.conf().get("javax.jdo.option.ConnectionURL"));

        DeltaTable.createOrReplace(spark)
                .tableName("default.delta_builder_partition_java")
                .addColumn("id", "INT")
                .addColumn("firstName", "STRING")
                .addColumn("middleName", "STRING")
                .addColumn(
                        DeltaTable.columnBuilder("lastName")
                                .dataType("STRING")
                                .comment("surname")
                                .build())
                .addColumn("gender", "STRING")
                .addColumn("birthDate", "TIMESTAMP")
                .addColumn("ssn", "STRING")
                .addColumn("salary", "INT")
                .partitionedBy("gender")
                .execute();
    }
}
