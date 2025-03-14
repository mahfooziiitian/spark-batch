package com.mahfooz.spark.dataset.rdd;

import com.mahfooz.spark.dataset.DataSets;
import com.mahfooz.spark.dataset.model.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class DatasetToRdd {

    public static void main(String[] args) {

        String fileName = args[0];

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName(DataSets.class.getSimpleName())
                .getOrCreate();

        // Create an RDD of Person objects from a text file
        JavaRDD<String> personRDD = sparkSession.read()
                .textFile(fileName)
                .javaRDD();


        JavaRDD<Person> peopleRDD = personRDD.map(line -> {
            String[] parts = line.split(",");
            Person person = new Person();
            person.setName(parts[0]);
            person.setAge(Integer.parseInt(parts[1].trim()));
            return person;
        });

        peopleRDD.foreach((Person person) -> System.out.println(person.getName()));
    }
}
