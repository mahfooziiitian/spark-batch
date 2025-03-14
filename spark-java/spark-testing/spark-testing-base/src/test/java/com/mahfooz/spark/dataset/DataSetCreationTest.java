package com.mahfooz.spark.dataset;

import com.google.common.collect.Lists;
import com.mahfooz.spark.context.SparkIntegrationTestBase;
import com.mahfooz.spark.model.Apple;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DataSetCreationTest extends SparkIntegrationTestBase {
    @Test
    public void testListOfEntities() {
        List<Apple> rows = Lists.newArrayList(new Apple("green", 70), new Apple("red", 110));
        final Dataset<Apple> actual = spark().createDataset(rows, Encoders.bean(Apple.class));
        actual.show(false);
    }

}
