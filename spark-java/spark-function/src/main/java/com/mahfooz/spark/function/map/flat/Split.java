package com.mahfooz.spark.function.map.flat;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;

public class Split implements FlatMapFunction<String, String> {

    @Override
    public Iterator<String> call(String s) {
        return  Arrays.asList(s.split(" ")).iterator();
    }
}