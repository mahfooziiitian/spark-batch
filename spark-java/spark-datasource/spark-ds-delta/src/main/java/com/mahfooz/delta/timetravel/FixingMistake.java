/**
 * Fix accidental deletes to a table for the user 111:
 */
package com.mahfooz.delta.timetravel;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FixingMistake {

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

        String yesterday = spark.sql("SELECT CAST(current_date() AS STRING)")
                .collectAsList()
                .get(0)
                .getString(0);

        Dataset<Row> df = spark.read().format("delta")
                .option("timestampAsOf", yesterday)
                .load(dataPath);

        df.show(false);
        System.exit(0);
        df.where("userId = 111")
                .write()
                .format("delta")
                .mode("append")
                .save(dataPath);
    }
}
