/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mahfooz.spark.sql.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SparkReadFile {

    public static void main(String[] args) {

        String logFile = SparkReadFile.class
                .getResource("/in/RealEstate.csv")
                .getFile();

        SparkSession spark = SparkSession.builder()
                .appName("SparkReadFile")
                .master("local")
                .getOrCreate();

        Dataset<String> logData = spark.read()
                .textFile(logFile);

        long numAs = logData.filter((String s) -> s.contains("a"))
                .count();

        long numBs = logData.filter((String s) -> s.contains("b"))
                .count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
        spark.stop();
    }
}
