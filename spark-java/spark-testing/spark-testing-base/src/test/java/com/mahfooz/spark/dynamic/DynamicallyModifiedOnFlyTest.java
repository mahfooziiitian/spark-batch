package com.mahfooz.spark.dynamic;

import com.mahfooz.spark.context.SparkIntegrationTestBase;
import com.mahfooz.spark.repository.ApplesRepository;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DynamicallyModifiedOnFlyTest extends SparkIntegrationTestBase {
    private final ApplesRepository repository = new ApplesRepository();

    @Test
    public void testMass_WhenWeightSpecified_ThenWeightsSum() {
        int expected = 85;
        Dataset<Row> input = getBulkData().withColumn("weight", lit(85));
        assertEquals(expected, repository.mass(input));
    }

    @Test
    public void testMass_WhenWeightIsNull_ThenNPE() {
        assertThrows(Exception.class, () -> {
            Dataset<Row> input = getBulkData().withColumn("weight", lit(null).cast(DataTypes.IntegerType));
            repository.mass(input);
        });
    }

    private Dataset<Row> getBulkData() {
        return spark().read().option("header", "true").csv(getTestDataFolder());
    }

    private String getTestDataFolder() {
        final String dataRoot = "D:/data/spark/csv/test";
        return new File(dataRoot).getPath();
    }
}
