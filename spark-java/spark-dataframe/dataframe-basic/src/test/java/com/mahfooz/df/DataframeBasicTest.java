package com.mahfooz.df;

import com.mahfooz.df.model.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataframeBasicTest {
    private static SparkSession spark;

    @BeforeAll
    public static void setUp() {
        SparkConf conf = new SparkConf()
                .setAppName("DataframeBasicTest")
                .setMaster("local[*]");
        spark = SparkSession.builder().config(conf).getOrCreate();
    }

    @AfterAll
    public static void tearDown() {
        spark.stop();
    }

    @Test
    public void testDataFrameOperations() {
        // Create a sample DataFrame
        Dataset<Row> dataFrame = spark.createDataFrame(
                Arrays.asList(
                        new Person("Alice", 25),
                        new Person("Bob", 30),
                        new Person("Charlie", 28)
                ),
                Person.class
        );

        // Perform some DataFrame operations
        Dataset<Row> result = dataFrame
                .withColumn("age_plus_10", col("age").plus(10))
                .filter(col("age_plus_10").geq(40));

        // Check the result
        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());

        Row firstRow = rows.get(0);
        System.out.println(firstRow);
        assertEquals("Bob", firstRow.getAs("name"));
        int age = firstRow.getAs("age");
        assertEquals(30, age);
    }
}