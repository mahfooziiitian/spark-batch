package com.mahfooz.df.ds;

import com.mahfooz.df.model.Person;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class DataframeFromDs {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("DataframeFromDs")
                .master("local[*]")
                .getOrCreate();

        // Sample data
        List<Person> persons = Arrays.asList(
                new Person(1,"Alice", 25),
                new Person(2,"Bob", 30),
                new Person(3,"Charlie", 35)
        );
        // Creating a dataset from the list of objects
        Dataset<Person> personDataset = spark.createDataset(
                persons,
                Encoders.bean(Person.class));

        // Creating a DataFrame from the dataset
        Dataset<Row> personDF = personDataset.toDF();

        // Show the DataFrame
        personDF.show();
    }
}
