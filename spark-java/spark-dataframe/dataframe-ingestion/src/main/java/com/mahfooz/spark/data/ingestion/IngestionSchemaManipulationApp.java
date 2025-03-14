package com.mahfooz.spark.data.ingestion;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import com.mahfooz.spark.data.util.DownloadFile;
import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

/**
 * CSV ingestion in a dataframe and manipulation.
 *
 * @author jgp
 */
public class IngestionSchemaManipulationApp {

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        IngestionSchemaManipulationApp app =
                new IngestionSchemaManipulationApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Restaurants in Wake County, NC")
                .master("local[*]")
                .getOrCreate();
        String dataHome = System.getenv("DATA_HOME") + "\\spark\\datasource\\csv";
        String downloadUrl = "https://raw.githubusercontent.com/jgperrin/net.jgp.books.spark.ch03/master/data/Restaurants_in_Wake_County_NC.csv";
        String filePath = dataHome + "\\Restaurants_in_Wake_County_NC.csv";

        //Download file if it is not exists
        if (!(new File(filePath).exists()))
            DownloadFile.downloadFile(downloadUrl, filePath);

        // Reads a CSV file with header, called
        // Restaurants_in_Wake_County_NC.csv,
        // stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load(filePath);

        System.out.println("*** Right after ingestion");
        df.show(5);
        df.printSchema();
        System.out.println("We have " + df.count() + " records.");

        // Let's transform our dataframe
        df = df.withColumn("county", lit("Wake"))
                .withColumnRenamed("HSISID", "datasetId")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY")
                .drop("OBJECTID")
                .drop("PERMITID")
                .drop("GEOCODESTATUS");
        df = df.withColumn("id", concat(
                df.col("state"),
                lit("_"),
                df.col("county"), lit("_"),
                df.col("datasetId")));

        // Shows at most 5 rows from the dataframe
        System.out.println("*** Dataframe transformed");
        df.show(5);

        // for book only
        Dataset<Row> dfUsedForBook = df.drop("address2")
                .drop("zip")
                .drop("tel")
                .drop("dateStart")
                .drop("geoX")
                .drop("geoY")
                .drop("address1")
                .drop("datasetId");
        dfUsedForBook.show(5, 15);
        // end

        df.printSchema();

        System.out.println("*** Looking at partitions");
        Partition[] partitions = df.rdd().partitions();
        int partitionCount = partitions.length;
        System.out.println("Partition count before repartition: " + partitionCount);

        df = df.repartition(4);
        System.out.println("Partition count after repartition: " + df.rdd().partitions().length);
    }
}