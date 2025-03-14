package com.mahfooz.spark.dataframe;

import com.google.common.collect.Lists;
import com.mahfooz.spark.context.SparkIntegrationTestBase;
import com.mahfooz.spark.model.Apple;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.lit;

public class DataFrameCreationTest extends SparkIntegrationTestBase {

    @Test
    public void testEmptyWithStructure() {
        final Dataset<Row> actual = spark().emptyDataFrame().withColumn("color", lit("green"));
        actual.printSchema();
    }

    @Test
    public void testSinglePrimitive() {
        Long value = 12L;
        List<Row> rows = Collections.singletonList(RowFactory.create(value));
        final Dataset<Row> actual = spark().createDataFrame(rows, Encoders.LONG().schema());
        actual.select("value").show(false);
    }

    @Test
    public void testPrimitiveList() {
        List<String> data = Lists.newArrayList("green", "red");
        Dataset<Row> actual = spark().createDataset(data, Encoders.STRING()).toDF("color");
        actual.show(false);
    }

    @Test
    public void testRowListWithAssignedSchema() {
        List<Row> rows = Arrays.asList(RowFactory.create("green"), RowFactory.create("red"));
        StructType schema = DataTypes.createStructType(
                new StructField[] { DataTypes.createStructField("color", DataTypes.StringType, false) });

        final Dataset<Row> actual = spark().createDataFrame(rows, schema);
        actual.show(false);
    }

    @Test
    public void testListOfEntities() {
        List<Apple> rows = Lists.newArrayList(new Apple("green", 70), new Apple("red", 110));
        final Dataset<Row> actual = spark().createDataFrame(rows, Apple.class);
        actual.show(false);
    }

}
