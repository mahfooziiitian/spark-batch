package com.mahfooz.spark.uaf.typesafe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;

public class TypeSafeAggregation {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[*]").appName("TypeSafeAggregation").getOrCreate();

        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        String path = "d:/data/spark/json/employees.json";
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
