package com.mahfooz.spark.function.pair.flat;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class PairFlatMapFunc implements PairFlatMapFunction<String,String,String> {

    @Override
    public Iterator<Tuple2<String, String>> call(String s) throws Exception {
        List<Tuple2<String, String>> pairs = new LinkedList<>();
        for (String word : s.split(" ")) {
            pairs.add(new Tuple2<>(word, word));
        }
        return pairs.iterator();
    }
}
