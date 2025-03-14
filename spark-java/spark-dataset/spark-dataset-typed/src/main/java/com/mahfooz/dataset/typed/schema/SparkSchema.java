/*

1. Create an RDD of Rows from the original RDD;
2. Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
3. Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.

*/
package com.mahfooz.dataset.typed.schema;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkSchema {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[*]").appName("SparkSchema").getOrCreate();

        // Create an RDD
        JavaRDD<String> peopleRDD = spark.sparkContext().textFile("d:/data/spark/text/people.txt", 1).toJavaRDD();

        // The schema is encoded in a string
        String schemaString = "name age";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        // Apply the schema to the RDD
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        // Creates a temporary view using the DataFrame
        peopleDataFrame.createOrReplaceTempView("people");

        // SQL can be run over a temporary view created using DataFrames
        Dataset<Row> results = spark.sql("SELECT name FROM people");

        // The results of SQL queries are DataFrames and support all the normal RDD
        // operations
        // The columns of a row in the result can be accessed by field index or by field
        // name
        Dataset<String> namesDS = results.map((MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        namesDS.show();
    }
}
