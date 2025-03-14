/*

an action triggers the lazy execution of all the transformations recorded.

Invoking an action operation will trigger the evaluation of all the transformations that preceded it, and it will either return some result to 
the driver or write data to a storage system, such as HDFS or the local file system.

Transformations					Actions                                                         

orderBy							show
groupBy							take
filter							count
select                          collect                                                          
join                            save

collect()

	Collects all the rows in the dataset from executors.
	All the rows will be sent from executors to the driver program.
	The collect action is usually used after the dataset is filtered down to a small dataset.

count()                        
	
	Returns the number of rows in the dataset.
	                         
first()                        
	
	Returns the first row in the dataset to the driver program.                          

take(n)
	
	Returns the first n rows in the dataset to the driver program. first() is equivalent to take(1).

reduce(func)
 	
 	Performs an aggregation on the rows in the dataset using the provided func. 
 	The provided func should follow the commutative and associative rule for the result to be correctly computed in parallel.                          takeSample(withReplacement, n, [seed])                        Randomly samples up to n rows with either a replacement or not and returns them to the driver program.                          takeOrdered(n, [ordering])                        Returns the first n rows in the dataset to the driver program and orders them by either natural ordering or custom ordering.                          top(n, [ordering])                        Returns the top n elements in the dataset.                          saveAsTextFile(path)                        Writes all the rows in the dataset as a text file into the provided directory. Each row will be converted to a string using the toString() method.

saveAsTextFile(path)
	
	Writes the content of RDD to a text file or a set of text files to local file system/HDFS.

foreach(func)
	
	Execute function for each data element in RDD.
	It usually used to update an accumulator or interacting with external systems.
	
	
 */
package spark.operation.action;

public class Action {

}
