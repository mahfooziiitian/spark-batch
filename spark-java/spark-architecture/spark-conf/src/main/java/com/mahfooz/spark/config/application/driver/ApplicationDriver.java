package com.mahfooz.spark.config.application.driver;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class ApplicationDriver {

    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf();
        sparkConf.set("spark.app.name","ApplicationDriver");
        sparkConf.set("spark.driver.cores","2");
        sparkConf.set("spark.master","local[*]");

        SparkSession sparkSession=SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        sparkSession.close();
    }
}
