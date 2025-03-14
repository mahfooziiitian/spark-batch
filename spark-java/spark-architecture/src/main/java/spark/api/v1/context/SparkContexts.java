/*

SparkContext is a class defined in the Spark library. It is the main entry point into the Spark library.

It represents a connection to a Spark cluster.

It is also required to create other important objects provided by the Spark API.

A Spark application must create an instance of the SparkContext class.

Currently, an application can have only one active instance of SparkContext.

To create another instance, it must first stop the active instance.

The SparkContext class provides multiple constructors.

The simplest one does not take any arguments.

An instance of the SparkContext class can be created as shown next.


Main entry point for Spark functionality

SparkContext can be utilized to create broadcast variables, RDDs, and accumulators, and denotes the connection to a Spark cluster.

To create a SparkContext, you first have to develop a SparkConf object that includes details about your application.

*/

package spark.api.v1.context;

public class SparkContexts {

}
