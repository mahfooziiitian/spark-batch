/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mahfooz.spark.api;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author 370037
 */
public class WordCount {
    
    public static void main(String [] args ){
        SparkConf sparkConf=new SparkConf().setMaster("local")
                .setAppName("Word Count");
        JavaSparkContext sparkContext=new JavaSparkContext(sparkConf);
        JavaRDD<String> inputFile=sparkContext.textFile(WordCount.class.getResource("/input.txt").getFile());
        JavaRDD<String> wordsFromFile=inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
        JavaPairRDD<String,Integer> countData=wordsFromFile.mapToPair(t-> new Tuple2(t,1))
        .reduceByKey((x,y)-> (int) x + (int) y );
        countData.saveAsTextFile("countData");
        sparkContext.close();
    }
}
