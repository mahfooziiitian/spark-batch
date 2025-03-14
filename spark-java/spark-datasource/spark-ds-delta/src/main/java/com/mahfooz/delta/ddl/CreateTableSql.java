package com.mahfooz.delta.ddl;

import org.apache.spark.sql.SparkSession;

public class CreateTableSql {

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

        spark.sql("CREATE OR REPLACE TABLE default.people10m (\n" +
                "  id INT,\n" +
                "  firstName STRING,\n" +
                "  middleName STRING,\n" +
                "  lastName STRING,\n" +
                "  gender STRING,\n" +
                "  birthDate TIMESTAMP,\n" +
                "  ssn STRING,\n" +
                "  salary INT\n" +
                ") USING DELTA");


    }
}
