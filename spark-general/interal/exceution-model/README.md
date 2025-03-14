# Spark Execution Model

## a. Logical Plan Creation

RDD / DataFrame / Dataset API: User code is converted into an Abstract Syntax Tree (AST).

Logical Plan Optimization: Catalyst Optimizer transforms the initial logical plan into an optimized plan.

## b. Physical Plan Execution

Physical Plan Creation: The optimized logical plan is converted into a DAG (Directed Acyclic Graph) of stages and tasks.

DAG Scheduler: Divides the DAG into stages and submits them to the Task Scheduler.

Task Scheduler: Assigns tasks to worker nodes (executors) based on locality and resource availability.

Execution in Executors: Executors run tasks in parallel, using JVM threads.

## Tungsten Execution Engine

Spark’s Tungsten engine optimizes:

1. Memory Management: Uses off-heap memory to reduce garbage collection.
2. Code Generation: Uses whole-stage code generation for better CPU utilization.
3. Vectorized Execution: Processes multiple rows at once for efficiency.
