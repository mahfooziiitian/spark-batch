package com.mahfooz.delta.history;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class DeltaTableHistory {

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
        String dataPath = String.format("%s/DataWarehouse/warehouse/save_as_table_partition_java",
                System.getenv("DATA_HOME").replace("\\","/"));

        DeltaTable deltaTable = DeltaTable.forPath(spark,dataPath);
        Dataset<Row> history = deltaTable.history();
        history.show(false);

        List<Row> latest_version = history.selectExpr("max(version)").collectAsList();
        Dataset<Row> df = spark.read().format("delta")
                .option("versionAsOf", latest_version.get(0).getLong(0)+"")
                .load(dataPath);
        df.show(false);

    }
}
