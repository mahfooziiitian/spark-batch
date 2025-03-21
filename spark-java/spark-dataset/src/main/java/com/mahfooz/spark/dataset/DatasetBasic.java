package com.mahfooz.spark.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple3;
import static org.apache.spark.sql.functions.col;
import java.util.Arrays;
import java.util.List;

public class DatasetBasic {

        public static void main(String[] args) {

                SparkSession spark = SparkSession.builder().appName("Dataset-Basic").master("local[4]").getOrCreate();

                List<Integer> data = Arrays.asList(10, 11, 12, 13, 14, 15);
                Dataset<Integer> ds = spark.createDataset(data, Encoders.INT());

                System.out.println("*** only one column, and it always has the same name");
                ds.printSchema();

                ds.show();

                System.out.println("*** values > 12");

                ds.show();

                List<Tuple3<Integer, String, String>> tuples = Arrays.asList(new Tuple3<>(1, "one", "un"),
                                new Tuple3<>(2, "two", "deux"), new Tuple3<>(3, "three", "trois"));

                Encoder<Tuple3<Integer, String, String>> encoder = Encoders.tuple(Encoders.INT(), Encoders.STRING(),
                                Encoders.STRING());

                Dataset<Tuple3<Integer, String, String>> tupleDS = spark.createDataset(tuples, encoder);

                System.out.println("*** Tuple Dataset types");
                tupleDS.printSchema();

                // the tuple columns have unfriendly names, but you can use them to query
                System.out.println("*** filter by one column and fetch another");
                tupleDS.where(col("_1").gt(2)).select(col("_2"), col("_3")).show();

                spark.stop();
        }
}
