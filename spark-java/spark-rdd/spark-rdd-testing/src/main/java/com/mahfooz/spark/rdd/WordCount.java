package com.mahfooz.spark.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public JavaPairRDD<String,Integer>  get(String url, JavaSparkContext javaSparkContext){
        return javaSparkContext.textFile(url)
                .flatMap(record -> Arrays.asList(record.split(" ")).iterator())
                .mapToPair(record->
                new Tuple2<String,Integer>(record,1))
                .reduceByKey((a,b)->a+b);
    }
}
