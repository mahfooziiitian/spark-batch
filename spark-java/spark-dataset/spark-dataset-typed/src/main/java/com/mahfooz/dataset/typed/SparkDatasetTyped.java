package com.mahfooz.dataset.typed;

import java.util.Arrays;
import java.util.Collections;

import com.mahfooz.dataset.typed.model.Person;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class SparkDatasetTyped {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("SparkDatasetTyped")
        .getOrCreate();

        // Create an instance of a Bean class
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);

        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), 
        personEncoder);
        javaBeanDS.show();

        // Encoders for most common types are provided in class Encoders
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map((MapFunction<Integer, Integer>) value -> value + 1,
                integerEncoder);
        transformedDS.show(); // Returns [2, 3, 4]

        spark.stop();
    }
}
