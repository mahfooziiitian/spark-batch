/*

In Java, functions are represented by classes implementing the interfaces in the org.apache.spark.api.java.function package.

 */
package com.mahfooz.spark.rdd.function.anonymous;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class RddAnonymousFunction {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("RddCaching").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("D:\\data\\spark\\text\\people.txt");
        JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
            public Integer call(String s) { return s.length(); }
        });
        int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) { return a + b; }
        });

        System.out.println(totalLength);
    }

}
