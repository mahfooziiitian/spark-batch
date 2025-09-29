# Fault Tolerance in Spark

## Lineage (DAG Recovery)

Lost partitions can be recomputed from parent RDDs.

## Checkpointing

Stores data to HDFS for resilience.

## Speculative Execution

Runs duplicate tasks if a task is slow.
