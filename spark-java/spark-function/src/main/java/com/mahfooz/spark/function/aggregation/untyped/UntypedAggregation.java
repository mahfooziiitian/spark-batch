package com.mahfooz.spark.function.aggregation.untyped;

import com.mahfooz.spark.function.config.DataFrameConfig;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class UntypedAggregation {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL user-defined DataFrames aggregation example")
                .getOrCreate();

        // $example on:untyped_custom_aggregation$
        // Register the function to access it
        //spark.udf().register("myAverage", udaf(new MyAverage(), Encoders.LONG()));

        Dataset<Row> df = spark.read().json(DataFrameConfig.PREFIX_DATA_PATH+"\\employees.json");
        df.createOrReplaceTempView("employees");
        df.show();

        Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
        result.show();

        spark.stop();
    }
}
