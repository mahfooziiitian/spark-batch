package com.mahfooz.spark.config.ui;

import com.mahfooz.spark.config.log.SparkLog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkUi {

    public static void main(String[] args) {

        SparkConf sparkConf=new SparkConf()
                .set("spark.ui.enabled","false")
                .setMaster("local[*]")
                .setAppName(SparkLog.class.getSimpleName());

        SparkSession sparkSession=SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        sparkSession.close();
    }
}
