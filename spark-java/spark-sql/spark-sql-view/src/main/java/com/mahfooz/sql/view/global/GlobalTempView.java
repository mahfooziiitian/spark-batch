/*

Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it terminates.
If you want to have a temporary view that is shared among all sessions and keep alive until the Spark 
application terminates, you can create a global temporary view.
Global temporary view is tied to a system preserved database global_temp, and we must use the qualified name to 
refer it, e.g. SELECT * FROM global_temp.view1.

*/
package com.mahfooz.sql.view.global;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GlobalTempView {

    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("GlobalTempView").getOrCreate();

        Dataset<Row> df = spark.read().json("d:/data/spark/json/people.json");
        // Register the DataFrame as a global temporary view
        df.createGlobalTempView("people");

        // Global temporary view is tied to a system preserved database `global_temp`
        spark.sql("SELECT * FROM global_temp.people").show();

        // Global temporary view is cross-session
        spark.newSession().sql("SELECT * FROM global_temp.people").show();

        spark.stop();

    }
}
