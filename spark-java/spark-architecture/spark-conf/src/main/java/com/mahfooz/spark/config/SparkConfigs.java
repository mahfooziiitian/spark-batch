package com.mahfooz.spark.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkConfigs {

    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf()
                .setMaster("local[*]")
                .setAppName(SparkConf.class.getSimpleName());

        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);

        javaSparkContext.stop();

    }
}
