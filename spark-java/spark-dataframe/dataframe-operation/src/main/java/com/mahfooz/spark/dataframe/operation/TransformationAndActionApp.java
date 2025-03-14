package com.mahfooz.spark.dataframe.operation;

import static org.apache.spark.sql.functions.expr;

import com.mahfooz.spark.dataframe.util.DownloadFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

/**
 * Transformation and action.
 *
 * @author jgp
 */
public class TransformationAndActionApp {

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        TransformationAndActionApp app = new TransformationAndActionApp();
        String mode = "noop";
        if (args.length != 0) {
            mode = args[0];
        }
        app.start(mode);
    }

    /**
     * The processing code.
     */
    private void start(String mode) {
        long t0 = System.currentTimeMillis();

        // Step 1 - Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Analysing Catalyst's behavior")
                .master("local[*]")
                .getOrCreate();
        long t1 = System.currentTimeMillis();
        System.out.println("1. Creating a session ........... " + (t1 - t0));
        String dataHome = System.getenv("DATA_HOME") + "\\spark\\datasource\\csv";
        String downloadUrl = "https://github.com/mahfooz-code/data/raw/main/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv";
        String filePath = dataHome + "\\NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv";

        //Download file if it is not exists
        if (!(new File(filePath).exists()))
            DownloadFile.downloadFile(downloadUrl, filePath);

        long t2= System.currentTimeMillis();
        System.out.println("2. Downloading file ........... " + (t2 - t1));
        // Step 2 - Reads a CSV file with header, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load(filePath);
        Dataset<Row> initialDf = df;
        long t3 = System.currentTimeMillis();
        System.out.println("3. Loading initial dataset ...... " + (t3 - t2));

        // Step 3 - Build a bigger dataset
        for (int i = 0; i < 10; i++) {
            df = df.union(initialDf);
        }
        long t4 = System.currentTimeMillis();
        System.out.println("4. Building full dataset ........ " + (t4 - t3));

        // Step 4 - Cleanup. preparation
        df = df.withColumnRenamed("Lower Confidence Limit", "lcl");
        df = df.withColumnRenamed("Upper Confidence Limit", "ucl");
        long t5 = System.currentTimeMillis();
        System.out.println("5. Clean-up ..................... " + (t5 - t4));

        // Step 5 - Transformation
        if (mode.compareToIgnoreCase("noop") != 0) {
            df = df
                    .withColumn("avg", expr("(lcl+ucl)/2"))
                    .withColumn("lcl2", df.col("lcl"))
                    .withColumn("ucl2", df.col("ucl"));
            if (mode.compareToIgnoreCase("full") == 0) {
                df = df
                        .drop(df.col("avg"))
                        .drop(df.col("lcl2"))
                        .drop(df.col("ucl2"));
            }
        }
        long t6 = System.currentTimeMillis();
        System.out.println("6. Transformations  ............. " + (t6 - t5));

        // Step 6 - Action
        df.collect();
        long t7 = System.currentTimeMillis();
        System.out.println("7. Final action ................. " + (t7 - t6));

        System.out.println("");
        System.out.println("# of records .................... " + df.count());
    }
}