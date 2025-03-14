package com.mahfooz.spark.function.pair;

import com.mahfooz.spark.function.map.flat.FlatMapFunctions;
import com.mahfooz.spark.function.map.flat.Split;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class PairFunctions {

    public static void main(String[] args) {

        SparkConf sparkConf=new SparkConf()
                .setMaster("local")
                .setAppName(FlatMapFunctions.class.getSimpleName());

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile("hdfs://...");
        JavaRDD<String> words = lines.flatMap(new Split());
        JavaPairRDD<String, Integer> ones = words.mapToPair(new CountMap());
        ones.collect().forEach(System.out::println);

    }
}
