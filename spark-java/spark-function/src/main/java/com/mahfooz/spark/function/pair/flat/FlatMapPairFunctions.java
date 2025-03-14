package com.mahfooz.spark.function.pair.flat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class FlatMapPairFunctions {

    public static void main(String[] args) {

        SparkConf sparkConf=new SparkConf()
                .setMaster("local")
                .setAppName(FlatMapPairFunctions.class.getSimpleName());

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello World!",
                "The quick brown fox jumps over the lazy dog."));

        JavaPairRDD<String, String> pairsRDD = rdd.flatMapToPair(new PairFlatMapFunc());

        pairsRDD.collect().forEach(System.out::println);
    }
}
