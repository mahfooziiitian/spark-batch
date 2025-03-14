package com.mahfooz.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WordCountTest {

    private  SparkConf sparkConf;
    private  JavaSparkContext javaSparkContext;
    private  WordCount wordcount=new WordCount();

    @BeforeEach
    public  void setupBeforeAll(){
        sparkConf=new SparkConf()
                .setAppName(WordCountTest.class.getSimpleName())
                .setMaster("local[*]");
        javaSparkContext=new JavaSparkContext(sparkConf);
    }

    @Test
    public void testRddCount(){
        String filePath="d:/data/spark/text/people.txt";
        long count=wordcount.get(filePath,javaSparkContext).count();
        assertEquals(6L,count);
    }

    @AfterEach
    public void tearDown(){
        javaSparkContext.close();
    }
}