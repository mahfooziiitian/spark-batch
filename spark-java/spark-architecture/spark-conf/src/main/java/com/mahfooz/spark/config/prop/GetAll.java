package com.mahfooz.spark.config.prop;

import org.apache.spark.SparkConf;

import java.util.Arrays;

public class GetAll {

    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf();
        Arrays.stream(sparkConf.getAll()).forEach(prop->{
            System.out.println(prop._1+"="+ prop._2);
        });
    }
}
