package com.mahfooz.spark.function.map.flat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FlatMapFunctions {

    public static void main(String[] args) {

        SparkConf sparkConf=new SparkConf()
                .setMaster("local")
                .setAppName(FlatMapFunctions.class.getSimpleName());

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile("hdfs://...");
        JavaRDD<String> words = lines.flatMap(new Split());


    }
}
