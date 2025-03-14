/*

Hive comes packaged with the Spark library as HiveContext that inherits from SQLContext.

Utilizing HiveContext, you can create and find tables in the HiveMetaStore and write queries on it using HiveQL.

When hive-site.xml is not configured, the context automatically produces a metastore named as metastore_db and a folder known as warehouse 
in the current directory.


Data Load from Hive to Spark
	
	sqlContext.sql("CREATE TABLE IF NOT EXISTS test.employee(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")

Data Load from Hive to Spark

	sqlContext.sql("LOAD DATA LOCAL INPATH 'employee.txt' INTO TABLE employee")

	val result = sqlContext.sql("FROM employee SELECT id, name, age")

	result.show

	
 */
package spark.component.sql.hive;

public class HiveConext {

}
