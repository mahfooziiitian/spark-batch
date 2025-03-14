package com.mahfooz.delta.ddl.generatedcolumn;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.types.DataTypes.DateType;

public class TableWithGeneratedColumn {

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

        DeltaTable.create(spark)
                .tableName("default.generated_column_java")
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
                .addColumn(
                        DeltaTable.columnBuilder("dateOfBirth")
                                .dataType(DateType)
                                .generatedAlwaysAs("CAST(birthDate AS DATE)")
                                .build())
                .addColumn("ssn", "STRING")
                .addColumn("salary", "INT")
                .partitionedBy("gender")
                .execute();
    }
}
