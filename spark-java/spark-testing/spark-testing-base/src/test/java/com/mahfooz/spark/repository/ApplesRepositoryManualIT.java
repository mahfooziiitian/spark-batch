package com.mahfooz.spark.repository;

import com.mahfooz.spark.context.SparkIntegrationTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ApplesRepositoryManualIT extends SparkIntegrationTestBase {

    private final ApplesRepository repository = new ApplesRepository();

    @Test
    public void testMass_WhenTwoApples_ThenSumOfWeights() {
        Integer[] weights = { 120, 150 };
        long expected = Arrays.stream(weights).reduce(0, (a, v) -> a + v);
        Dataset<Row> df = spark().createDataset(Arrays.asList(weights), Encoders.INT()).toDF("weight");

        long actual = repository.mass(df);

        assertEquals(expected, actual);
    }

}
