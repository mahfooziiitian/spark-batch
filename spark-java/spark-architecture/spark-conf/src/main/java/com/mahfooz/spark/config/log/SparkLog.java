package com.mahfooz.spark.config.log;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkLog {

    public static void main(String[] args) {

        SparkConf sparkConf=new SparkConf()
                .set("spark.logConf","false")
                .setMaster("local[*]")
                .setAppName(SparkLog.class.getSimpleName());

        SparkSession sparkSession=SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        sparkSession.close();
    }
}
