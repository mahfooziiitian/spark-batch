package com.mahfooz.spark.function.vargs.two;

import org.apache.spark.api.java.function.Function2;

public class Func2 implements Function2<Integer, Integer, Integer> {

    @Override
    public Integer call(Integer i1, Integer i2) {
        return i1 + i2;
    }
}
