package com.mahfooz.dataset.rdd.creation;

import com.mahfooz.dataset.model.Person;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDatasetFromRdd {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("SparkDatasetFromRdd").getOrCreate();

        // Create an RDD of Person objects from a text file
        JavaRDD<Person> peopleRDD = spark.read().textFile("d:/data/spark/text/people.txt").javaRDD().map(line -> {
            String[] parts = line.split(",");
            Person person = new Person();
            person.setName(parts[0]);
            person.setAge(Integer.parseInt(parts[1].trim()));
            return person;
        });
        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        // Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("people");

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

        // The columns of a row in the result can be accessed by field index
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF
                .map((MapFunction<Row, String>) row -> "Name: " + row.getString(0), stringEncoder);
        teenagerNamesByIndexDF.show();

        spark.stop();
    }
}
