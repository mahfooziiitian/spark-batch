package com.mahfooz.spark.rdd;

import com.google.common.collect.Lists;
import com.mahfooz.spark.context.SparkIntegrationTestBase;
import com.mahfooz.spark.model.Apple;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.List;

public class RDDCreationTest extends SparkIntegrationTestBase {
    @Test
    public void testEmptyRDD() {
        jsc().emptyRDD();
    }

    @Test
    public void testPrimitiveList() {
        List<String> data = Lists.newArrayList("green", "red");
        jsc().parallelize(data);
    }

    @Test
    public void testListOfEntities() {
        List<Apple> rows = Lists.newArrayList(new Apple("green", 70), new Apple("red", 110));
        JavaRDD<Apple> result = jsc().parallelize(rows);
        result.foreach(v -> System.out.println(v));
    }

}
