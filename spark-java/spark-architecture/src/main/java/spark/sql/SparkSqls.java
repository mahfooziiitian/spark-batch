/*
Reading Spark SQL from Hive
	
	//drop the table if it already exists in the same name.

spark.sql("DROP TABLE IF EXISTS sample_hive_table")

//save as a hive table

spark.table("sample_table").write.saveAsTable("sample_hive_table")

// query on hive table 

val resultsHiveDF = spark.sql("SELECT name, rollno,totalmark,division FROM sample_hive_table WHERE totalmark > 40000")

resultsHiveDF.show(10)

*/
package spark.sql;

public class SparkSqls {
	
}