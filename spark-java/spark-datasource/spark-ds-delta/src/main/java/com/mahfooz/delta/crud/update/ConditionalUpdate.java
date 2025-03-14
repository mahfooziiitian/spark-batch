package com.mahfooz.delta.crud.update;
import io.delta.tables.*;
import org.apache.spark.sql.*;

import java.util.HashMap;

public class ConditionalUpdate {

    public static void main(String[] args) {
        String dataHome = System.getenv("DATA_HOME");

        SparkSession spark = SparkSession.builder()
                .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .master("local[*]").getOrCreate();

        Dataset<Row> data = spark.range(0, 5).toDF();

        data.write().format("delta").mode(SaveMode.Overwrite).save(String.format("%s/FileData/Delta/crud/update/delta-update-table",dataHome));

        DeltaTable deltaTable = DeltaTable.forPath(String.format("%s/FileData/Delta/crud/update/delta-update-table",dataHome));

        // Update every even value by adding 100 to it
        deltaTable.update(
                functions.expr("id % 2 == 0"),
                new HashMap<>() {{
                    put("id", functions.expr("id + 100"));
                }}
        );

        // Delete every even value
        deltaTable.delete(functions.expr("id % 2 == 0"));

        // Upsert (merge) new data
        Dataset<Row> newData = spark.range(0, 20).toDF();

        deltaTable.as("oldData")
                .merge(
                        newData.as("newData"),
                        "oldData.id = newData.id")
                .whenMatched()
                .update(
                        new HashMap<>() {{
                            put("id", functions.col("newData.id"));
                        }})
                .whenNotMatched()
                .insertExpr(
                        new HashMap<>() {{
                            put("id", String.valueOf(functions.col("newData.id")));
                        }}
                )
                .execute();

        deltaTable.toDF().show();
    }
}
