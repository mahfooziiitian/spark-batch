package com.mahfooz.spark.rdd.function.concrete;

import org.apache.spark.api.java.function.Function2;

public class Sum implements Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer a, Integer b) {
        return a + b;
    }
}