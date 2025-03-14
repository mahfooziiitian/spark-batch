package com.mahfooz.ds.generic.csv;

import com.mahfooz.ds.generic.util.DownloadFile;
import com.mahfooz.ds.generic.util.SchemaInspector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;

/**
 * CSV ingestion in a dataframe with a Schema.
 *
 * @author jgp
 */
public class ComplexCsvToDataframeWithSchemaApp {

    public static final DecimalType$ DecimalType = DecimalType$.MODULE$;

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        ComplexCsvToDataframeWithSchemaApp app =
                new ComplexCsvToDataframeWithSchemaApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Complex CSV with a schema to Dataframe")
                .master("local[*]")
                .getOrCreate();

        // Creates the schema
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField(
                        "id",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "authordId",
                        DataTypes.IntegerType,
                        true),
                DataTypes.createStructField(
                        "bookTitle",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "releaseDate",
                        DataTypes.DateType,
                        true), // nullable, but this will be ignored
                DataTypes.createStructField(
                        "url",
                        DataTypes.StringType,
                        false) });

        // GitHub version only: dumps the schema
        SchemaInspector.print(schema);

        String dataHome = System.getenv("DATA_HOME") + "\\spark\\datasource\\csv";
        String downloadUrl = "https://raw.githubusercontent.com/jgperrin/net.jgp.books.spark.ch07/master/data/books.csv";
        String filePath = dataHome + "\\books.csv";
        if (!(new File(filePath).exists()))
            DownloadFile.downloadFile(downloadUrl, filePath);

        // Reads a CSV file with header, called books.csv, stores it in a
        // dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ";")
                .option("dateFormat", "MM/dd/yyyy")
                .option("quote", "*")
                .schema(schema)
                .load(filePath);

        // GitHub version only: dumps the schema
        SchemaInspector.print("Schema ...... ", schema);
        SchemaInspector.print("Dataframe ... ", df);

        // Shows at most 20 rows from the dataframe
        df.show(30, 25, false);
        df.printSchema();
    }
}