/*

A DataFrame is the most common Structured API and simply represents a table of data with rows and columns.

The list of columns and the types in those columns the schema. A simple analogy would be a spreadsheet with named columns

A DataFrame is a distributed collection of data organized into named columns.

It is conceptually equivalent to a table in a relational database or a data frame in R/Python, but with richer optimizations under the hood.

DataFrames can be constructed from a wide array of sources such as: structured data files, tables in Hive, external databases, or existing RDDs.

The DataFrame API is available in Scala, Java, Python, and R.

Dataframe allows data selection, filtering, and aggregation.

Since data frames store more information about the structure of the data, the processing is done efficiently as compared to RDD.

DataFrame is similar to relational database tables with rows of named columns.

	API availability in different programming languages
	Hive compatibility
	Scales from KiloBytes data to Petabytes data
	Easy integration with Big Data tools and applications

Creating DataFrame
	With a SparkSession, applications can create DataFrame easily from

	an existing RDD
	a hive table
	Spark data sources.
	To create a DataFrame from a List or Seq using spark.createDataFrame:

		val df = spark.createDataFrame(List(("John", 35), ("Thomas", 30), ("Martin", 15)))
	Here spark mentioned is the SparkSession object.

	To create a DataFrame based on the content of a JSON file:

		val df = spark.read.json("examples/src/main/resources/data.json")

DataFrame Operations
	
	To select people older than 31,

	df.filter($"age" > 31).show()

Querying DataFrames
	
	val df =  spark.read.json("examples/src/main/resources/data.json")

	df.createOrReplaceTempView("data")

	val sqlDF = spark.sql("SELECT * FROM data")
	 sqlDF.show()

DataFrames across Sessions

	 val df =  spark.read.json("examples/src/main/resources/data.json")
	df.createGlobalTempView("data")
	val sqlDF = spark.sql("SELECT * FROM data")
	sqlDF.show()


====================================================================================================================

 Step 1 of 5 
 
1) Create a SparkSession object.
	
	a) Enter the Spark shell.

		spark-shell

	b) Import the SparkSession package

		import org.apache.spark.sql.SparkSession

	c) Create a SparkSession object.

		val spark= SparkSession.builder.appName("My Spark Application").master("local[*]").
		config("spark.sql.warehouse.dir", "/root/spark-warehouse").getOrCreate

2) Create a DataFrame

	val langPercentDF = spark.createDataFrame(List(("Scala", 35), ("Python", 30), ("R", 15), ("Java", 20)))
	
3) Display the values in DataFrame
	
	langPercentDF.show()
4) Rename the columns
	
	val lpDF = langPercentDF.withColumnRenamed("_1", "language").withColumnRenamed("_2", "percent")
	lpDF.show()
5) DataFrame in descending order of percentage
	
	lpDF.orderBy(desc("percent")).show(false)

6)	

	
 */
package spark.api.dataframe;

public class DataFrames {

}
