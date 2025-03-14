package com.mahfooz.df.schema.manual;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class DfSchemaManual {
    public static void main(String[] args) {
        SparkSession spark= SparkSession
                .builder()
                .master("local[*]")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("ORIGIN_COUNTRY_NAME", StringType, true),
                        DataTypes.createStructField("DEST_COUNTRY_NAME", StringType, true),
                        DataTypes.createStructField("count", LongType, false)
                }
        );

        String dataPath = System.getenv("DATA_HOME")+"\\FileData\\Csv\\Flight\\2010-summary.csv";

        Dataset<Row> df = spark.read().schema(schema).format("csv")
                .load(dataPath);

        df.show(false);
    }
}
