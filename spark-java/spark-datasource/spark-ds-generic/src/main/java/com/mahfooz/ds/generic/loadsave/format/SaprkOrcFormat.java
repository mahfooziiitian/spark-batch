/*

Since Spark 2.3, Spark supports a vectorized ORC reader with a new ORC file format for ORC files. 
The vectorized reader is used for the native ORC tables (e.g., the ones created using the clause USING ORC) 
when spark.sql.orc.impl is set to native and spark.sql.orc.enableVectorizedReader is set to true.
For the Hive ORC serde tables (e.g., the ones created using the clause USING HIVE OPTIONS (fileFormat 'ORC'))
the vectorized reader is used when spark.sql.hive.convertMetastoreOrc is also set to true.

*/
package com.mahfooz.ds.generic.loadsave.format;

import org.apache.spark.sql.SparkSession;

public class SaprkOrcFormat {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
        .master("local[*]").appName("SparkGenericLoad").getOrCreate();

        //Dataset<Row> usersDF = spark.read().format("orc")
        //.option("spark.sql.orc.enabled", "hive")
        //.load("d:/data/spark/orc/users.orc");

        //usersDF.write().format("orc").option("orc.bloom.filter.columns", "favorite_color")
        //        .option("orc.dictionary.key.threshold", "1.0").save("users_with_options.orc");

        spark.stop();

    }
}
