package com.mahfooz.delta.ddl.partition;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class PartitionTable {

    public static void main(String[] args) {
        String dataHome = System.getenv("DATA_HOME");
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

        String inputDataPath = String.format("%s/FileData/Csv/state_crime.csv",dataHome.replace("\\","/"));

        Dataset<Row> df =spark.read().format("csv")
                .option("header", true)
                .load(inputDataPath);

        df.printSchema();

        df.write().format("delta")
                .partitionBy("Year")
                .mode(SaveMode.Overwrite)
                .saveAsTable("default.save_as_table_partition_java");
    }
}
