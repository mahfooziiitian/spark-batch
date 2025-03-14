# Spark Ui

The Spark UI is a web application designed to help with monitoring and debugging
Spark applications.
It contains detailed runtime information and various resource
consumptions of a Spark application.
The runtime includes various metrics that are tremendously helpful in diagnosing performance issues in your Spark applications.
One thing to note is that the Spark UI is available only while a Spark application is running.
The navigation bar at the top of the Spark UI contains links to the various tabs including jobs, stages, storage, environment, executors, and SQL.

## Spark Application UI: 
    
    http://localhost:4040/

## Resource Manager: 
    
    http://localhost:9870

## Spark JobTracker: 
    
    http://localhost:8088/

## Node Specific Info: 
    
    http://localhost:8042/

## Environment

The Environment tab contains the basic information about the environment that a
Spark application is running in.
The sections are Runtime Information, Spark Properties, System Properties, and Classpath Entries.

| Name                | Description                                                                                                                                                                                                 |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Runtime Information | This section contains the locations and versions of the various components that Spark depends on, including Java and Scala.                                                                                 |
| Spark properties    | This area contains the basic and advanced properties that are configured in a Spark application. The basic properties include the basic information about an application such as application ID, name, etc. |
| System Properties   | These properties are mainly at the OS and Java level and are not Spark specific.                                                                                                                            |
| Classpath Entries   | This area contains a list of classpath and JAR files that are used in a Spark application.                                                                                                                  |

## Executor

The Executors tab contains the summary and breakdown information for each of the executors that is supporting a Spark application.
This information includes the capacity of certain resources as well as how much is being used in each executor.
The resources include memory, disk, CPU, and so on.
The Summary section provides a bird's-eye view of the resource consumption across all the executors in a Spark application.
