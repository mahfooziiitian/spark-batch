package com.mahfooz.spark.function.vargs.two;

import com.mahfooz.spark.function.map.flat.Split;
import com.mahfooz.spark.function.pair.CountMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Function2s {

    public static void main(String[] args) {

        SparkConf sparkConf=new SparkConf()
                .setMaster("local")
                .setAppName(Function2s.class.getSimpleName());

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("Hello World!",
                "The quick brown fox jumps over the lazy dog."));

        JavaRDD<String> words = lines.flatMap(new Split());

        JavaPairRDD<String, Integer> ones = words.mapToPair(new CountMap());

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Func2());

        counts.collect().forEach(System.out::println);

    }
}
