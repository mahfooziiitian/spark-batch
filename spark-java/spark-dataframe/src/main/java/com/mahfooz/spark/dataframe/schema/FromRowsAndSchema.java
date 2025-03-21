package com.mahfooz.spark.dataframe.schema;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;


// Note that conceptually a DataFrame is a DataSet<Row>, bot the Java API
// doesn't actually have a definition of DataFrame.

// Create a Spark Dataset<Row> from a list of Row instances and a schema
// constructed explicitly. Query it.

// This example is fundamental for Dataset<Row> as the schema is created
// explicitly instead of being inferred via an Encoder like in the Dataset
// examples.

public class FromRowsAndSchema {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("FromRowsAndSchema")
                .master("local[4]")
                .getOrCreate();

        List<Row> customerRows = Arrays.asList(
                RowFactory.create(1, "Widget Co", 120000.00, 0.00, "AZ"),
                RowFactory.create(2, "Acme Widgets", 410500.00, 500.00, "CA"),
                RowFactory.create(3, "Widgetry", 410500.00, 200.00, "CA"),
                RowFactory.create(4, "Widgets R Us", 410500.00, 0.0, "CA"),
                RowFactory.create(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
        );

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("sales", DataTypes.DoubleType, true),
                DataTypes.createStructField("discount", DataTypes.DoubleType, true),
                DataTypes.createStructField("state", DataTypes.StringType, true)
        );
        StructType customerSchema = DataTypes.createStructType(fields);

        Dataset<Row> customerDF =
                spark.createDataFrame(customerRows, customerSchema);

        System.out.println("*** the schema created");
        customerDF.printSchema();

        System.out.println("*** the data");
        customerDF.show();

        System.out.println("*** just the rows from CA");
        customerDF.filter(col("state").equalTo("CA")).show();

        spark.stop();
    }
}