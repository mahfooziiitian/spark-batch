package com.mahfooz.spark.dataframe.ds;

import com.mahfooz.spark.dataframe.model.Cube;
import com.mahfooz.spark.dataframe.model.Square;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class JavaSQLDataSource {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL data sources example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        runBasicDataSourceExample(spark);
        runGenericFileSourceOptionsExample(spark);

        spark.stop();
    }

    private static void runGenericFileSourceOptionsExample(SparkSession spark) {
        // $example on:ignore_corrupt_files$
        // enable ignore corrupt files
        spark.sql("set spark.sql.files.ignoreCorruptFiles=true");
        // dir1/file3.json is corrupt from parquet's view
        Dataset<Row> testCorruptDF = spark.read().parquet(
                "examples/src/main/resources/dir1/",
                "examples/src/main/resources/dir1/dir2/");
        testCorruptDF.show();
        // +-------------+
        // |         file|
        // +-------------+
        // |file1.parquet|
        // |file2.parquet|
        // +-------------+
        // $example off:ignore_corrupt_files$
        // $example on:recursive_file_lookup$
        Dataset<Row> recursiveLoadedDF = spark.read().format("parquet")
                .option("recursiveFileLookup", "true")
                .load("examples/src/main/resources/dir1");
        recursiveLoadedDF.show();
        // +-------------+
        // |         file|
        // +-------------+
        // |file1.parquet|
        // |file2.parquet|
        // +-------------+
        // $example off:recursive_file_lookup$
        spark.sql("set spark.sql.files.ignoreCorruptFiles=false");
        // $example on:load_with_path_glob_filter$
        Dataset<Row> testGlobFilterDF = spark.read().format("parquet")
                .option("pathGlobFilter", "*.parquet") // json file should be filtered out
                .load("examples/src/main/resources/dir1");
        testGlobFilterDF.show();
        // +-------------+
        // |         file|
        // +-------------+
        // |file1.parquet|
        // +-------------+
        // $example off:load_with_path_glob_filter$
    }

    private static void runBasicDataSourceExample(SparkSession spark) {
        // $example on:generic_load_save_functions$
        Dataset<Row> usersDF = spark.read().load("examples/src/main/resources/users.parquet");
        usersDF.select("name", "favorite_color").write().save("namesAndFavColors.parquet");
        // $example off:generic_load_save_functions$
        // $example on:manual_load_options$
        Dataset<Row> peopleDF =
                spark.read().format("json").load("examples/src/main/resources/people.json");
        peopleDF.select("name", "age").write().format("parquet").save("namesAndAges.parquet");
        // $example off:manual_load_options$
        // $example on:manual_load_options_csv$
        Dataset<Row> peopleDFCsv = spark.read().format("csv")
                .option("sep", ";")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("examples/src/main/resources/people.csv");
        // $example off:manual_load_options_csv$
        // $example on:manual_save_options_orc$
        usersDF.write().format("orc")
                .option("orc.bloom.filter.columns", "favorite_color")
                .option("orc.dictionary.key.threshold", "1.0")
                .option("orc.column.encoding.direct", "name")
                .save("users_with_options.orc");
        // $example off:manual_save_options_orc$
        // $example on:direct_sql$
        Dataset<Row> sqlDF =
                spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`");
        // $example off:direct_sql$
        // $example on:write_sorting_and_bucketing$
        peopleDF.write().bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed");
        // $example off:write_sorting_and_bucketing$
        // $example on:write_partitioning$
        usersDF
                .write()
                .partitionBy("favorite_color")
                .format("parquet")
                .save("namesPartByColor.parquet");
        // $example off:write_partitioning$
        // $example on:write_partition_and_bucket$
        peopleDF
                .write()
                .partitionBy("favorite_color")
                .bucketBy(42, "name")
                .saveAsTable("people_partitioned_bucketed");
        // $example off:write_partition_and_bucket$

        spark.sql("DROP TABLE IF EXISTS people_bucketed");
        spark.sql("DROP TABLE IF EXISTS people_partitioned_bucketed");
    }




}
