This is an overview of the steps:

1. Write DataFrame/Dataset/SQL Code.
2. If valid code, Spark converts this to a Logical Plan. 
3. Spark transforms this Logical Plan to a Physical Plan, checking for optimizations along the way. 
4. Spark then executes this Physical Plan (RDD manipulations) on the cluster.

To execute code, we must write code. 

This code is then submitted to Spark either through the console or via a submitted job. 

This code then passes through the Catalyst Optimizer, which decides how the code should be executed and lays out a plan for doing so before, finally, the code is run and the result is returned to the user.


# job

A parallel computation consisting of multiple tasks that gets spawned in response to a Spark action.

1. Spark Job Status
2. DAG Visualization
3. Completed Stages

A Spark job is a sequence of stages that are composed of tasks.

# Stages

Each Wide Transformation results in a separate Number of Stages.

# tasks
The number of tasks you could see in each stage is the number of partitions that spark 
is going to work on and each task inside a stage is the same work that will be done by 
spark but on a different partition of data.

a task is a unit of execution in Spark that is assigned to a partition of data.


# storage

The Storage tab displays the persisted RDDs and DataFrames, if any, in the application. 
The summary page shows the storage levels, sizes and partitions of all RDDs, and the details page shows the sizes and using executors for all partitions in an RDD or DataFrame.