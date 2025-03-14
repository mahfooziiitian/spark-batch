package com.mahfooz.spark.function.user;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import static org.apache.spark.sql.functions.udf;
import org.apache.spark.sql.types.DataTypes;


public class JavaUserDefinedScalar {

    public static void main(String[] args) {

        // $example on:udf_scalar$
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark SQL UDF scalar example")
                .getOrCreate();

        // Define and register a zero-argument non-deterministic UDF
        // UDF is deterministic by default, i.e. produces the same result for the same input.
        UserDefinedFunction random = udf(
                () -> Math.random(), DataTypes.DoubleType
        );
        random.asNondeterministic();
        spark.udf().register("random", random);
        spark.sql("SELECT random()").show();


        // Define and register a one-argument UDF
        spark.udf().register("plusOne", new UDF1<Integer, Integer>() {
            @Override
            public Integer call(Integer x) {
                return x + 1;
            }
        }, DataTypes.IntegerType);

        spark.sql("SELECT plusOne(5)").show();


        // Define and register a two-argument UDF
        UserDefinedFunction strLen = udf(
                (String s, Integer x) -> s.length() + x, DataTypes.IntegerType
        );
        spark.udf().register("strLen", strLen);
        spark.sql("SELECT strLen('test', 1)").show();

        // UDF in a WHERE clause
        spark.udf().register("oneArgFilter", new UDF1<Long, Boolean>() {
            @Override
            public Boolean call(Long x) {
                return  x > 5;
            }
        }, DataTypes.BooleanType);
        spark.range(1, 10).createOrReplaceTempView("test");
        spark.sql("SELECT * FROM test WHERE oneArgFilter(id)").show();


        // $example off:udf_scalar$
        spark.stop();
    }
}