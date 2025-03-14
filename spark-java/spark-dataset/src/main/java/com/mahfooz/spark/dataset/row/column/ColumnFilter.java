package com.mahfooz.spark.dataset.row.column;

import com.mahfooz.spark.dataset.row.DatasetRow;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ColumnFilter {

    public static void main(String[] args) {

        String fileName = args[0];

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName(DatasetRow.class.getSimpleName())
                .getOrCreate();

        // Create an Dataset of Person objects from a text file
        Dataset<Row> dataset = sparkSession.read()
                .option("header", true)
                .csv(fileName);

        Column count=dataset.col("count");
        Column destinationCountry=dataset.col("DEST_COUNTRY_NAME");

        Dataset<Row> filterDataset=dataset.filter(count.equalTo("1").and(destinationCountry.equalTo("Croatia")));

        filterDataset.show();
    }
}
