package com.mahfooz.spark.function.map.flat;

import org.apache.spark.api.java.function.FlatMapFunction2;

import java.util.Iterator;

public class FlatMapFunc2 implements FlatMapFunction2<String, String,Integer> {

    @Override
    public Iterator<Integer> call(String s, String s2) throws Exception {
        return null;
    }
}