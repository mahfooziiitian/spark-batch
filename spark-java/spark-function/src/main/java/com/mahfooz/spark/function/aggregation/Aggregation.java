package com.mahfooz.spark.function.aggregation;

import com.mahfooz.spark.function.config.DataFrameConfig;
import org.apache.spark.sql.*;

public class Aggregation {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Aggregation")
                .getOrCreate();

        // $example on:typed_custom_aggregation$
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        String path = DataFrameConfig.PREFIX_DATA_PATH+ "\\employees.json";
        Dataset<Employee> ds = spark.read().json(path).as(employeeEncoder);
        ds.show();

        MyAverage myAverage = new MyAverage();

        // Convert the function to a `TypedColumn` and give it a name
        TypedColumn<Employee, Double> averageSalary = myAverage.toColumn().name("average_salary");
        Dataset<Double> result = ds.select(averageSalary);
        result.show();

        spark.stop();
    }
}
