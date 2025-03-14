# Spark application

Spark applications run as independent sets of processes on a cluster, coordinated by the SparkContext object in your
main program
(called the driver program).

A Spark application consists of two parts.

1. The first is the application data processing logic expressed using Spark APIs.
2. The other is the Spark driver.

## Spark Driver

The Spark driver is the central coordinator of a Spark application, and it interacts with a cluster manager
to figure out which machines to run the data processing logic on.

For each one of those machines, the Spark driver requests that the cluster manager launch a process called the
Spark executor.

Another important job of the Spark driver is to manage and distribute Spark tasks onto each executor on behalf of the
application.

If the data processing logic requires the Spark driver to display the computed results to a user, then it will
coordinate with each Spark executor to collect the computed result and merge them together.

Spark driver is the master and the Spark executor is the slave.
Each of these components runs as an independent process on a Spark cluster.

### SparkSession

SparkSession provides a single point of entry to interact with Spark data.
SparkSession is available since Spark 2.0 and provides a simpler, higher-level API for working with Spark.
It encapsulates SparkContext and includes functionality provided by SQLContext, HiveContext, and StreamingContext.

The entry point into a Spark application is through a class called SparkSession,
which provides facilities for setting up configurations as well as APIs for expressing data
processing logic.
SparkSession was introduced in Spark 2.0 to provide a unified entry point for reading data, executing SQL queries, and
working with Datasets and DataFrames.

It encapsulates the functionality of SQLContext, HiveContext, and StreamingContext available in earlier versions of Spark. SparkSession provides a higher-level API compared to SparkContext.

1. Unified Interface: SparkSession provides a unified interface for reading data, executing SQL queries, and working with structured data using Datasets and DataFrames.
2. Hive Integration: SparkSession has built-in support for working with Hive tables and Hive SQL.
3. DataFrames and Datasets: SparkSession provides APIs for working with Datasets and DataFrames, which are distributed collections of data organized into named columns.
4. Optimized Performance: SparkSession optimizes the execution plan of queries for better performance.

The session object has information 
1. Spark Master
2. The Spark application
3. The configuration options

Spark applications can use multiple sessions to use different underlying data catalogs.

### SparkContext

SparkContext is the entry point of the Spark session.
It is your connection to the Spark cluster and can be used to create RDDs, accumulators,
and broadcast variables on that cluster.
Specifically, to run on a cluster, the SparkContext can connect to several types of cluster managers (either Spark’s own
standalone
cluster manager, Mesos or YARN), which allocate resources across applications.
Once connected, Spark acquires executors on nodes in the cluster, which are processes that run computations and store
data for your
application.
Next, it sends your application code (defined by JAR or Python files passed to SparkContext) to the executors.
Finally, SparkContext sends tasks to the executors to run.
It is preferable to have one SparkContext active per JVM, and hence you should
call stop() on the active SparkContext before you create a new one. You might have noticed previously that in the local
mode, whenever we start a Python or Scala shell we have a SparkContext object created automatically and the variable sc
refers to the SparkContext object.

We did not need to create the SparkContext, but instead started using it to create RDDs from text files.
The process running the main() function of the application and creating the SparkContext.

SparkContext is the entry point for low-level Spark APIs and represents the connection to a Spark cluster. It was the main entry point for Spark applications in versions prior to 2.0.

1. Low-Level API: SparkContext provides a low-level API for creating Resilient Distributed Datasets (RDDs), which are the fundamental data structure in Spark.
2. Fine-Grained Control: SparkContext offers fine-grained control over Spark configurations and allows you to set various parameters for Spark applications.
3. Legacy APIs: SparkContext is used for working with older RDD-based APIs and streaming applications using StreamingContext.

Underlying SparkContext will be the same for both sessions as you can have only one context per Spark application.
Only one SparkContext should be active per JVM.
You must stop() the active SparkContext before creating a new one.

## Relationship between SparkSession and SparkContext

1. Encapsulation: SparkSession encapsulates SparkContext. When you create a SparkSession, it internally creates a SparkContext for you. You don't need to create a separate SparkContext when you are using SparkSession.
2. Higher-Level API: While SparkContext provides a low-level, RDD-based API, SparkSession builds on top of SparkContext and provides a higher-level, more user-friendly API for working with structured data.
3. Data Source: When you work with structured data using Datasets and DataFrames, you interact with SparkSession. It allows you to read data from various sources, execute SQL queries, and perform operations on structured data.
4. Backward Compatibility: SparkContext is still accessible within SparkSession for backward compatibility. You can access the underlying SparkContext object if needed.

## Executor

Each Spark executor is a JVM process and is exclusively allocated to a specific Spark
application.
This was a conscious design decision to avoid sharing a Spark executor
between multiple Spark applications in order to isolate them from each other so one badly
behaving Spark application would not affect other Spark applications.
The lifetime of a  Spark executor is the duration of a Spark application, which could run for a few minutes or
for a few days.
Since Spark applications are running in separate Spark executors, sharing
data between them will require writing the data to an external storage system like HDFS.
Each application gets its own executor processes, which stay up for the duration of the whole application and run tasks
in multiple threads.
This has the benefit of isolating applications from each other, on both the scheduling side (each driver schedules its
own tasks) and executor side (tasks from different applications run in different JVMs).
However, it also means that data cannot be shared across different Spark applications (instances of SparkContext)
without writing it to an external storage system.

Spark is agnostic to the underlying cluster manager.

As long as it can acquire executor processes, and these communicate with each other, it is relatively easy to run it
even on a cluster manager that also supports other applications (e.g. Mesos/YARN).

The driver program must listen for and accept incoming connections from its executors throughout its lifetime (e.g., see
spark.driver.port in the network config section).
As such, the driver program must be network addressable from the worker nodes.
Because the driver schedules tasks on the cluster, it should be run close to the worker nodes, preferably on the same
local area network.

If you’d like to send requests to the cluster remotely, it’s better to open an RPC to the driver and have it submit
operations from nearby than to run a driver far away from the worker nodes.

Playing the slave role, each
Spark executor does what it is told, which is to execute the data processing logic in the
form of tasks. Each task is executed on a separate CPU core.
In addition to executing assigned tasks, each Spark executor has the responsibility of caching a portion
of the data in memory and/or on disk when it is told to do so by the application logic.

## Use cases for spark application

The following is a small list of applications that were developed using Spark:
• Customer intelligence applications
• Data warehouse solutions
• Real-time streaming solutions
• Recommendation engines
• Log processing
• User-facing services
• Fraud detection
