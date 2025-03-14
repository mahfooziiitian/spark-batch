package com.mahfooz.spark.rdd.function.concrete;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ConcreteFunctionRdd {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RddCaching").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("D:\\data\\spark\\text\\people.txt");
        JavaRDD<Integer> lineLengths = lines.map(new GetLength());
        int totalLength = lineLengths.reduce(new Sum());
        System.out.println(totalLength);
    }
}
