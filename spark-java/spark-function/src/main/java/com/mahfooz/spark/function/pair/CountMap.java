package com.mahfooz.spark.function.pair;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class CountMap implements PairFunction<String, String, Integer> {

    @Override
    public Tuple2<String, Integer> call(String s) {
        return new Tuple2(s, 1);
    }

}
