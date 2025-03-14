/*

A DataFrame is a Dataset organized into named columns.

 */
package com.mahfooz.spark.dataframe;

import com.mahfooz.spark.dataframe.model.Customer;
import com.mahfooz.spark.dataframe.model.StateSales;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;

import java.util.Arrays;
import java.util.List;

public class DataFrames {

<<<<<<< HEAD:Spark/Spark/Java/spark-dataframe/src/main/java/com/mahfooz/spark/dataframe/DataFrames.java
        public static void main(String[] args) {
            SparkSession spark = SparkSession
                    .builder()
                    .appName("DataFrames")
                    .master("local[*]")
                    .getOrCreate();
=======
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFrames")
                .master("local[*]")
                .getOrCreate();
>>>>>>> 82d936b783289e15283aa19e45bfcacb190528ba:spark/spark2/java/spark-dataframe/src/main/java/com/mahfooz/spark/dataframe/DataFrames.java


        // The Java API requires you to explicitly instantiate an encoder for
        // any JavaBean you want to use for schema inference

        Encoder<Customer> custEncoder = Encoders.bean(Customer.class);

        // Create a container of the JavaBean instances

        List<Customer> data = Arrays.asList(
                new Customer(1, "Widget Co", 120000.00, 0.00, "AZ"),
                new Customer(2, "Acme Widgets", 410500.00, 500.00, "CA"),
                new Customer(3, "Widgetry", 410500.00, 200.00, "CA"),
                new Customer(4, "Widgets R Us", 410500.00, 0.0, "CA"),
                new Customer(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
        );

<<<<<<< HEAD:Spark/Spark/Java/spark-dataframe/src/main/java/com/mahfooz/spark/dataframe/DataFrames.java
            // Use the encoder and the container of JavaBean instances to create a Dataset
            Dataset<Customer> ds = spark.createDataset(data, custEncoder);

            System.out.println("*** here is the schema inferred from the Customer bean");
            ds.printSchema();
=======
        // Use the encoder and the container of JavaBean instances to create a Dataset

        Dataset<Customer> ds = spark.createDataset(data, custEncoder);

        System.out.println("*** here is the schema inferred from the Cust bean");
        ds.printSchema();
>>>>>>> 82d936b783289e15283aa19e45bfcacb190528ba:spark/spark2/java/spark-dataframe/src/main/java/com/mahfooz/spark/dataframe/DataFrames.java

        System.out.println("*** here is the data");
        ds.show();

<<<<<<< HEAD:Spark/Spark/Java/spark-dataframe/src/main/java/com/mahfooz/spark/dataframe/DataFrames.java
            // Querying a Dataset of any type results in a
            // DataFrame (i.e. Dataset<Row>)
=======
        // Querying a Dataset of any type results in a
        // DataFrame (i.e. Dataset<Row>)
>>>>>>> 82d936b783289e15283aa19e45bfcacb190528ba:spark/spark2/java/spark-dataframe/src/main/java/com/mahfooz/spark/dataframe/DataFrames.java

        Dataset<Row> smallerDF =
                ds.select("sales", "state").filter(col("state").equalTo("CA"));

        System.out.println("*** here is the dataframe schema");
        smallerDF.printSchema();

        System.out.println("*** here is the data");
        smallerDF.show();


        // But a Dataset<Row> can be converted back to a Dataset of some other
        // type by using another bean encoder

        Encoder<StateSales> stateSalesEncoder = Encoders.bean(StateSales.class);

        Dataset<StateSales> stateSalesDS = smallerDF.as(stateSalesEncoder);

        System.out.println("*** here is the schema inferred from the StateSales bean");
        stateSalesDS.printSchema();

        System.out.println("*** here is the data");
        stateSalesDS.show();

        spark.stop();
    }
}
