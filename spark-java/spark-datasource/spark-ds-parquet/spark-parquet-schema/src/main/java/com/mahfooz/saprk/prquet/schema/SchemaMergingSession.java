package com.mahfooz.saprk.prquet.schema;

import com.mahfooz.saprk.prquet.model.Cube;
import com.mahfooz.saprk.prquet.model.Square;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class SchemaMergingSession {

    public static void main(String[] args) {

        SparkSession spark=SparkSession.builder()
                .master("local[*]")
                .appName("SchemaMergingSession")
                .config("spark.sql.parquet.mergeSchema", true)
                .getOrCreate();

        List<Square> squares = new ArrayList<>();
        for (int value = 1; value <= 5; value++) {
            Square square = new Square();
            square.setValue(value);
            square.setSquare(value * value);
            squares.add(square);
        }

        // Create a simple DataFrame, store into a partition directory
        Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
        String dataHome = System.getenv("DATA_HOME");
        String basePath = String.format("%s/FileData/Parquet/schema/schema-merge", dataHome);

        squaresDF.write().parquet(basePath+"/key=1");

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
        cubesDF.write().parquet(basePath+"/key=2");

        // Read the partitioned table
        Dataset<Row> mergedDF = spark.read().parquet(basePath);
        mergedDF.printSchema();

        mergedDF.show(100,false);

        spark.stop();

    }
}
