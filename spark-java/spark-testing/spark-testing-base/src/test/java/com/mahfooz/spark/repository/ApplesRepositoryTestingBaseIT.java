package com.mahfooz.spark.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class ApplesRepositoryTestingBaseIT extends SharedJavaSparkContext {

    private final ApplesRepository repository = new ApplesRepository();

    @Test
    public void testMass_WhenTwoApples_ThenSumOfWeights() {
        Integer[] weights = { 120, 150 };
        long expected = Arrays.stream(weights).reduce(0, (a, v) -> a + v);
        SparkSession spark = SparkSession.builder().config(conf()).getOrCreate();
        Dataset<Row> df = spark.createDataset(Arrays.asList(weights), Encoders.INT()).toDF("weight");

        long actual = repository.mass(df);

        assertEquals(expected, actual);
    }

}
