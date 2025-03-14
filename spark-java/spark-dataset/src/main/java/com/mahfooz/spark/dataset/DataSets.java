/*

Dataset is denoted by Dataset[T].

 */

package com.mahfooz.spark.dataset;

import com.mahfooz.spark.dataset.model.Person;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSets {

        public static void main(String[] args) {

                String fileName = args[0];

                SparkSession sparkSession = SparkSession.builder().master("local")
                                .appName(DataSets.class.getSimpleName()).getOrCreate();

                // Create an RDD of Person objects from a text file
                JavaRDD<String> personRDD = sparkSession.read().textFile(fileName).javaRDD();

                JavaRDD<Person> peopleRDD = personRDD.map(line -> {
                        String[] parts = line.split(",");
                        Person person = new Person();
                        person.setName(parts[0]);
                        person.setAge(Integer.parseInt(parts[1].trim()));
                        return person;
                });

                // Apply a schema to an RDD of JavaBeans to get a DataFrame
                Dataset<Row> peopleDF = sparkSession.createDataFrame(peopleRDD, Person.class);

                // Register the DataFrame as a temporary view
                peopleDF.createOrReplaceTempView("people");

                // SQL statements can be run by using the sql methods provided by sqlContext
                Dataset<Row> teenagersDF = sparkSession.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

                // The columns of a row in the result can be accessed by field index
                Encoder<String> stringEncoder = Encoders.STRING();
                Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(row -> "Name: " + row.getString(0),
                                stringEncoder);

                teenagerNamesByIndexDF.show();

                Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(row -> "Name: " + row.<String>getAs("name"),
                                stringEncoder);
                teenagerNamesByFieldDF.show();
        }
}