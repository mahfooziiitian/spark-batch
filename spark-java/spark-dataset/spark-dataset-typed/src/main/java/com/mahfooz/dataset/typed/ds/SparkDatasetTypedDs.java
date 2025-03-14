package com.mahfooz.dataset.typed.ds;

import com.mahfooz.dataset.typed.model.Person;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class SparkDatasetTypedDs {
    
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("SparkDatasetTypedDs")
        .getOrCreate();
        
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        // DataFrames can be converted to a Dataset by providing a class. Mapping based
        // on name
        String path = "d:/data/spark/json/people.json";
        Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
        peopleDS.show();

        spark.stop();
    }
}
