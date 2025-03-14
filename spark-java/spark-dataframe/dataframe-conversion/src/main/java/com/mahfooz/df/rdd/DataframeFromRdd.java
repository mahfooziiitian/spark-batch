package com.mahfooz.df.rdd;

import com.mahfooz.df.model.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataframeFromRdd {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Dataframe From RDD")
                .getOrCreate();
        String data_file = System.getenv("DATA_HOME")+"/FileData/Text/people.txt";

        // Create an RDD of Person objects from a text file
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile(data_file)
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setId(Integer.parseInt(parts[0].trim()));
                    person.setName(parts[1]);
                    person.setAge(Integer.parseInt(parts[2].trim()));
                    return person;
                });

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        peopleDF.explain();
        peopleDF.show();
    }
}
