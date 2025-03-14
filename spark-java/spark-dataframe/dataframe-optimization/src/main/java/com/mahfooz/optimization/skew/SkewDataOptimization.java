package com.mahfooz.optimization.skew;

import org.apache.spark.sql.SparkSession;

public class SkewDataOptimization {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .getOrCreate();

        String tripDataFile = String.format("%s/FileData/Parquet/trips/*.parquet",System.getenv("DATA_HOME"));

        spark.read()
                .parquet(tripDataFile)
                .describe()
                .show(false);

    }
}
