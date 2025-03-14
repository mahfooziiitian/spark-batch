# Spark dag

Spark’s high-level scheduling layer uses RDD dependencies to build a Directed Acyclic Graph (a DAG) of stages for each Spark job.

In the Spark API, this is called the DAG Scheduler.

As you have probably noticed, errors that have to do with connecting to your cluster, your configuration 
parameters, or launching a Spark job show up as DAG Scheduler errors.

This is because the execution of a Spark job is handled by the DAG.

The DAG builds a graph of stages for each job, determines the locations to run each task, and passes that information on to the TaskScheduler, which is responsible for running tasks on the cluster.

The TaskScheduler creates a graph with dependencies between partitions.
