package com.mahfooz.ds.modes.reading.malformed;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.functions.*;

public class MalformedMode {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("MalformedMode")
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

        Dataset<Row> df = spark.read()
                .option("mode", "DROPMALFORMED")
                .option("header", "true")
                .format("csv")
                .schema(myManualSchema)
                .load(data_file);

        System.out.println(df.count());

        df.filter(col("DEST_COUNTRY_NAME").equalTo("United States"))
                //.filter(col("ORIGIN_COUNTRY_NAME").equalTo("Grenada"))
                .show(1000);

    }
}
