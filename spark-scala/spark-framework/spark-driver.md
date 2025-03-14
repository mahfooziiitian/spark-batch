Program that produces the SparkContext, connecting to a given Spark Master.
Declares the actions and transformations on RDDs of data.

1. At the core (and part) of every Spark application is a Spark driver program, which creates a SparkSession
   object for you.
   The Driver converts your Spark application into a single or multiple batch of Spark jobs during interactive sessions with Spark
   shells.

2. The driver transforms each job into a Directed Acyclic Graph (DAG).
   Spark’s execution plan, where each node within a DAG could be a single or multiple Spark stages.
   The driver is the process "in the driver seat" of your Spark Application.
3. It is the controller of the execution of a Spark Application and maintains all of the state of the Spark cluster
   (the state and tasks of the executors). 

4. The process running the main() function of the application and creating the SparkContext.

The driver is the process "in the driver seat" of your Spark Application.

It is the controller of the execution of a Spark Application and maintains all of the state of the Spark
cluster (the state and tasks of the executors).

It must interface with the cluster manager in order to actually get physical resources and launch executors.

At the end of the day, this is just a process on a physical machine that is responsible for maintaining the
state of the application running on the cluster.
The Spark driver is the central coordinator of a Spark application, and it interacts with a cluster manager
to figure out which machines to run the data processing logic on

Another important job of the Spark driver is to manage and distribute

Spark tasks onto each executor on behalf of the application.

If the data processing logic requires the Spark driver to display the computed results to a user, then it will
coordinate with each Spark executor to collect the computed result and merge them
together.

The entry point into a Spark application is through a class called SparkSession,
which provides facilities for setting up configurations as well as APIs for expressing data
processing logic.


For each one of those machines, the Spark driver requests that the cluster manager launch a process called the Spark executor. 

Another important job of the Spark driver is to manage and distribute Spark tasks onto each executor on behalf of the application. 

If the data processing logic requires the Spark driver to display the computed results to a user, then it will coordinate with each Spark executor to collect the computed result and merge them together


# SparkContext
What is SparkContext?

1. The driver program use the SparkContext to connect and communicate with the cluster and it helps in executing and coordinating the Spark job with the resource managers like YARN or Mesos.
2. Using SparkContext you can actually get access to other contexts like  SQLContext and HiveContext. 
3. Using SparkContext we can set configuration parameters to the Spark job.


# What is a SQLContext?
SQLContext is your gateway to SparkSQL. 
Here is how you create a SQLContext using the SparkContext.

sc is an existing SparkContext.
   
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)

Once you have the SQLContext you can start working with DataFrame, DataSet etc.


# Hive context
HiveContext is your gateway to Hive. 
HiveContext has all the functionalities of a SQLContext. 
If you look at the API documentation you can see that HiveContext extends SQLContext, meaning, it has support the functionalities that SQLContext support plus more.
   
      public class HiveContext extends SQLContext implements Logging


# SparkSession

SparkSession was introduced in Spark 2.0 to make it easy for the developers, so we don’t have worry about different contexts and to streamline the access to different contexts.
By having access to SparkSession, we automatically have access to the SparkContext.

      val spark = SparkSession
      .builder()
      .appName("hirw-test")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


SparkSession is now the new entry point of Spark that replaces the old SQLContext and HiveContext.

Note that the old SQLContext and HiveContext are kept for backward compatibility.

      val spark = SparkSession
      .builder()
      .appName("hirw-hive-test")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()