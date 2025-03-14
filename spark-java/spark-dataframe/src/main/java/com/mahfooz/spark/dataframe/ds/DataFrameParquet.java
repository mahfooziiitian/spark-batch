package com.mahfooz.spark.dataframe.ds;

import com.mahfooz.spark.dataframe.config.DataFrameConfig;
import com.mahfooz.spark.dataframe.model.Cube;
import com.mahfooz.spark.dataframe.model.Square;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;

public class DataFrameParquet {

    public static void main(String[] args) throws AnalysisException {
        // $example on:init_session$
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Dataframe Parquet")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        // $example off:init_session$

        runBasicParquetExample(spark);
        runParquetSchemaMergingExample(spark);

        spark.stop();
    }
    private static void runBasicParquetExample(SparkSession spark) {
        // $example on:basic_parquet_example$
        Dataset<Row> peopleDF = spark.read().json(DataFrameConfig.PREFIX_DATA_PATH+"\\Json\\people.json");

        // DataFrames can be saved as Parquet files, maintaining the schema information
        peopleDF.write().mode(SaveMode.Overwrite).parquet(DataFrameConfig.PREFIX_DATA_PATH+"\\Parquet\\people.parquet");

        // Read in the Parquet file created above.
        // Parquet files are self-describing so the schema is preserved
        // The result of loading a parquet file is also a DataFrame
        Dataset<Row> parquetFileDF = spark.read().parquet(DataFrameConfig.PREFIX_DATA_PATH+"\\Parquet\\people.parquet");

        // Parquet files can also be used to create a temporary view and then used in SQL statements
        parquetFileDF.createOrReplaceTempView("parquetFile");
        Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19");
        Dataset<String> namesDS = namesDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        namesDS.show();
    }


    private static void runParquetSchemaMergingExample(SparkSession spark) {
        // $example on:schema_merging$
        List<Square> squares = new ArrayList<>();
        for (int value = 1; value <= 5; value++) {
            Square square = new Square();
            square.setValue(value);
            square.setSquare(value * value);
            squares.add(square);
        }

        // Create a simple DataFrame, store into a partition directory
        Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
        squaresDF.write().mode(SaveMode.Overwrite).parquet(DataFrameConfig.PREFIX_DATA_PATH+"\\Parquet\\test_table/key=1");

        List<Cube> cubes = new ArrayList<>();
        for (int value = 6; value <= 10; value++) {
            Cube cube = new Cube();
            cube.setValue(value);
            cube.setCube(value * value * value);
            cubes.add(cube);
        }

        // Create another DataFrame in a new partition directory,
        // adding a new column and dropping an existing column
        Dataset<Row> cubesDF = spark.createDataFrame(cubes, Cube.class);
        cubesDF.write().mode(SaveMode.Overwrite).parquet(DataFrameConfig.PREFIX_DATA_PATH+"\\Parquet\\test_table\\key=2");

        // Read the partitioned table
        Dataset<Row> mergedDF = spark.read().option("mergeSchema", true)
                .parquet(DataFrameConfig.PREFIX_DATA_PATH+"\\Parquet\\test_table");
        mergedDF.printSchema();

        // The final schema consists of all 3 columns in the Parquet files together
        // with the partitioning column appeared in the partition directory paths
        // root
        //  |-- value: int (nullable = true)
        //  |-- square: int (nullable = true)
        //  |-- cube: int (nullable = true)
        //  |-- key: int (nullable = true)
        // $example off:schema_merging$
    }


}
