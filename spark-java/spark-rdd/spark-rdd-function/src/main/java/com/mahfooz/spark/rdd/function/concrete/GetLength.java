package com.mahfooz.spark.rdd.function.concrete;

import org.apache.spark.api.java.function.Function;

public class GetLength implements Function<String, Integer> {

    @Override
    public Integer call(String s) {
        return s.length();
    }
}