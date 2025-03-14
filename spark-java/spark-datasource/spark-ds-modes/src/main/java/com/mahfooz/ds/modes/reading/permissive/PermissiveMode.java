package com.mahfooz.ds.modes.reading.permissive;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class PermissiveMode {

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
                .option("mode", "PERMISSIVE")
                .option("header", "true")
                .schema(myManualSchema)
                .load(data_file);

        System.out.println(df.count());
    }
}
