package com.mahfooz.spark.dataframe.operation;

import static org.apache.spark.sql.functions.expr;

import com.mahfooz.spark.dataframe.util.DownloadFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

/**
 * Transformation explained.
 *
 * @author jgp
 */
public class TransformationExplainApp {

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        TransformationExplainApp app = new TransformationExplainApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {

        // Step 1 - Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Showing execution plan")
                .master("local")
                .getOrCreate();

        String dataHome = System.getenv("DATA_HOME") + "\\spark\\datasource\\csv";
        String downloadUrl = "https://github.com/mahfooz-code/data/raw/main/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv";
        String filePath = dataHome + "\\NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv";

        //Download file if it is not exists
        if (!(new File(filePath).exists()))
            DownloadFile.downloadFile(downloadUrl, filePath);

        // Step 2 - Reads a CSV file with header, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load(filePath);
        Dataset<Row> df0 = df;

        // Step 3 - Build a bigger dataset
        df = df.union(df0);

        // Step 4 - Cleanup. preparation
        df = df.withColumnRenamed("Lower Confidence Limit", "lcl");
        df = df.withColumnRenamed("Upper Confidence Limit", "ucl");

        // Step 5 - Transformation
        df = df
                .withColumn("avg", expr("(lcl+ucl)/2"))
                .withColumn("lcl2", df.col("lcl"))
                .withColumn("ucl2", df.col("ucl"));

        // Step 6 - explain - Spark v2
        df.explain();

        // Step 6 - explain - Spark v3

        // simple
        // df.explain("simple");
        // extended
        // df.explain("extended");
        // codegen
        // df.explain("codegen");
        // cost
        // df.explain("cost");
        // formatted
        //df.explain("formatted");

    }
}