# Spark Driver

1. seperate process to execute user applications
2. create SparkContext to schedule jobs execution and negotiate with cluster manager.


1. RDD graph
2. DAG Scheduler
3. TaskScheduler
4. Scheduler Backend

## SparkContext

1. It represents the connection to spark cluser and can be used to create RDDs, accumulators and broadcasts variable.

## DAG Scheduler

1. Computes a dag of stages of each job and submit them to TaskScheduler determines the preferred location for tasks (based on cache status or shuffle file locations) and find minimum schedule to run the jobs.

## TaskScheduer

1. responsible for sending tasks to clsuter, running them, retrying if there are failures and mitigating straggler.

## Schedule Backend


