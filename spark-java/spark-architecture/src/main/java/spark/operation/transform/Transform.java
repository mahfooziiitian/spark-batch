/*

Transformations, as the name suggests, transform Spark’s RDDs or DataFrame into a new RDD or a DataFrame without altering the original data, 
giving the original data the property of immutability.

Transformation operations are lazily evaluated, meaning Spark will delay the evaluations of the invoked operations until an action is taken

In other words, the transformation operations merely record the specified transformation logic and will apply them at a later point.

All transformations are evaluated lazily.

Transformations are functions that use an RDD as the input and return one or more RDDs as the output.


Filter
	
	It takes a function and return RDD.

map(func)
	
	This applies the provided function to each row as iterating through the rows in the dataset.
	The returned RDD will contain whatever the provided func returns.
	
	val stringList = Array("Spark is awesome","Spark is cool")
	val stringRDD = sc.parallelize(stringList)
	val allCapsRDD = stringRDD.map(line => line.toUpperCase)
	allCapsRDD.collect().foreach(println)
	
	def toUpperCase(line:String) : String = { line.toUpperCase }
	stringRDD.map(l => toUpperCase(l)).collect.foreach(println)
	

flatMap(func)
	
	Similar to map(func), the func should return a collection rather than a single element, and this method will flatten out the returned 
	collection. This allows an input item to map to zero or more output items.
	
	val wordRDD = stringRDD.flatMap(line => line.split(" ")
	wordRDD.collect().foreach(println)

filter(func)
	
	Only the elements that the func function returns true will be collected in the returned RDD.
	In other words, collect only the rows that meet the condition defined in the given func function.
	
	val awesomeLineRDD = stringRDD.filter(line => line.contains("awesome"))
	awesomeLineRDD.collect

mapPartitions(func)

	Similar to map(func), but this applies at the partition (chunk) level.
	This requires the func function to take the input as an iterator to iterate through each row in the partition.
	The method signature of the given func must be func(Iterator[T]) => Iterator[U]), which means it takes an iterator of type T and returns 
	an iterator of type U, where type U and type T don’t necessarily have to the same.
	
	import scala.util.Random
	val sampleList = Array("One", "Two", "Three", "Four","Five")
	val sampleRDD = sc.parallelize(sampleList, 2)
	val result = sampleRDD.mapPartitions((itr:Iterator[String]) => { val rand = new Random(System.currentTimeMillis + Random.nextInt) 
	itr.map(l => l + ":" + rand.nextInt) }
	
	
	import scala.util.Random
	def addRandomNumber(rows:Iterator[String]) = {
	  val rand = new Random(System.currentTimeMillis + Random.nextInt)
	  rows.map(l => l + " : " + rand.nextInt)
	}
	
	val result = sampleRDD.mapPartitions((rows:Iterator[String]) => addRandomNumber(rows))
	
	
mapParitionsWithIndex(func)
	
	This is similar to mapPartitions, but an additional partition index number is provided to the func function.
	val numberRDD = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10), 2)
	numberRDD.mapPartitionsWithIndex((idx:Int, itr:Iterator[Int]) => {	itr.map(n => (idx, n) ) }).collect()
	
union(otherRDD)
	
	This transformation does what it sounds like.
	It combines the rows in the source RDD with otherRDD.
	
	val	rdd1=sc.parallelize(Array(1,2,3,4,5))
	val	rdd2=sc.parallelize(Array(1,6,7,8))
	val	rdd3=rdd1.union(rdd2)
	rdd3.collect()
	
intersection(otherRDD)
	
	Only the rows that exist in both the source RDD and otherRDD are returned.
	
	val rdd1 = sc.parallelize(Array("One", "Two", "Three"))
	val rdd2 = sc.parallelize(Array("two","One","threed","One"))
	val rdd3 = rdd1.intersection(rdd2)
	rdd3.collect()
	
substract(otherRDD)
	This subtracts the rows in otherRDD from the source RDD.

distinct([numTasks])
	
	This removes duplicate rows from the source RDD.

sample(withReplace, fraction, seed)
	This is usually used to reduce a large dataset to a smaller one by randomly selecting a fraction of rows using the given seed and with or 
	without replacements.

reduceByKey



 */
package spark.operation.transform;

public class Transform {

}
