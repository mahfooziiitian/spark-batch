package com.mahfooz.dataset.untyped;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class UntypedDataset {

    public static void main(String[] args) {

        String jsonFilePath="D:/data/spark/json/people.json";
        SparkSession spark = SparkSession
        .builder()
        .appName("UntypedDataset")
        .master("local[*]")
        .getOrCreate();

        Dataset<Row> df = spark.read().json(jsonFilePath);
        
        // Displays the content of the DataFrame to stdout
        df.show();

        // Print the schema in a tree format
        df.printSchema();
        
        // Select only the "name" column
        df.select("name").show();
        
        // Select everybody, but increment the age by 1
        df.select(col("name"), col("age").plus(1)).show();
        
        // Select people older than 21
        df.filter(col("age").gt(21)).show();
        
        // Count people by age
        df.groupBy("age").count().show();
    }
}
