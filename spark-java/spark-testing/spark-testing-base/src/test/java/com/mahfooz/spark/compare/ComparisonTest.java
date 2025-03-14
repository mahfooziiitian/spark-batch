package com.mahfooz.spark.compare;

import com.mahfooz.spark.context.SparkIntegrationTestBase;
import com.mahfooz.spark.model.Apple;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import com.google.common.collect.Lists;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ComparisonTest extends SparkIntegrationTestBase {

    @Test
    public void testStructure() {
        List<Apple> data = Lists.newArrayList(new Apple("Green", 85));
        Dataset<Row> df = spark().createDataFrame(data, Apple.class);

        assertEquals(Encoders.bean(Apple.class).schema(), df.schema());
    }

    @Test
    public void testOneValue() {
        Apple apple = new Apple("Green", 85);
        List<Apple> data = Lists.newArrayList(apple);
        Dataset<Row> df = spark().createDataFrame(data, Apple.class);

        Integer actual = df.first().getAs("weight");
        assertEquals(apple.getWeight(), actual);
    }

    @Test
    public void testPrimitivesList() {
        List<String> data = Lists.newArrayList("green", "red");
        Dataset<Row> df = spark().createDataset(data, Encoders.STRING()).toDF("color");
        List<String> actual = df.select("color").as(Encoders.STRING()).collectAsList();

        assertEquals(actual.size(), 2);
    }

    @Test
    public void testDataFrames() {
        List<Apple> data = Lists.newArrayList(new Apple("Green", 85));
        Dataset<Row> expected = spark().createDataFrame(data, Apple.class);
        Dataset<Row> actual = spark().createDataFrame(data, Apple.class);

        assertEquals(0, expected.except(actual).count());
    }
}
