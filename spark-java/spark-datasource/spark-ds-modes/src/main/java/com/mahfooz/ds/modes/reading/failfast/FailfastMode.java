package com.mahfooz.ds.modes.reading.failfast;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class FailfastMode {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("FailfastMode")
                .master("local[*]")
                .getOrCreate();

        StructType myManualSchema = new StructType(new StructField[]{
                DataTypes.createStructField("DEST_COUNTRY_NAME", StringType, true),
                DataTypes.createStructField("ORIGIN_COUNTRY_NAME", StringType, true),
                DataTypes.createStructField("count", LongType, false)}
        );

        String data_file = String.format(
                "%s/FileData/Csv/2010-summary-malformed.csv",
                System.getenv("DATA_HOME")
        );

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("mode", "FAILFAST")
                .schema(myManualSchema)
                .option("inferSchema", "true")
                .load(data_file);

        df.show(100);
        System.out.println(df.count());
    }
}
