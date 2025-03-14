package com.mahfooz.spark.binary;

import org.apache.spark.sql.SparkSession;

public class SparkDsBinary {

    public static void main(String[] args) {

        String master = args[0]; // Assuming the master configuration is passed as the first command line argument
        String appName = args[1]; // Assuming the application name is passed as the second command line argument
        String fileFormat = getConfiguredFileFormat(); // Assuming a method getConfiguredFileFormat() is implemented to retrieve the configured file format
        String pathGlobFilter = args[2]; // Assuming the path glob filter is passed as the third command line argument

        SparkSession spark=SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .getOrCreate();

        spark.read().format(fileFormat).option("pathGlobFilter", pathGlobFilter).load("/path/to/data");

    }

    private static String getConfiguredFileFormat() {
        return "csv";
    }
}
