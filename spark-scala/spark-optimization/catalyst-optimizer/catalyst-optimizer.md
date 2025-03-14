# QueryPlan
QueryPlan is part of Catalyst to build a tree of relational operators of a structured query.

A QueryPlan has an output attributes (that serves as the base for the schema), a collection of expressions and a schema.

QueryPlan has statePrefix that is used when displaying a plan with ! to indicate an invalid plan, and ' to indicate an unresolved plan.

A QueryPlan is invalid if there are missing input attributes and children subnodes are non-empty.

A QueryPlan is unresolved if the column names have not been verified and column types have not been looked up in the Catalog.

A QueryPlan has zero, one or more Catalyst expressions.

# Logical Plan

1. A logical plan is a tree that represents both schema and data. 
2. These trees are manipulated and optimized by a catalyst framework.

# Unresolved logical plan or Parsed logical plan

We call it an Unresolved Logical Plan because the column or table names may be inaccurate or 
even exist when we have a valid code and correct syntax.

Spark creates a blank Logical Plan at this step where there are no checks for the column name, table name

This plan is generated post verifying that everything is correct on the syntactic field.


# Resolved or Analysed logical plan

After the generation of an unresolved plan, it will resolve everything unresolved yet by 
accessing an internal Spark structure mentioned as "Catalog".

"Catalog" is a repository of Spark table, DataFrame, and DataSet.
The data from meta-store is pulled into an internal storage component of Spark (also known as Catalog).

The analyzer can reject the Unresolved Logical Plan when it is not able to resolve them (column name, table name, etc.). 

It creates a Resolved Logical Plan if it is able to resolve them.

Upon successful completion of everything, the plan is marked as "Analyzed Logical Plan".


# Optimized logical plan

In order to resolve the Analyzed logical plans, they are sent through a series of rules. 

The optimized logical plan is produced as a result. 

Spark is normally allowed to plug in a set of optimization rules by the optimized logical plan.

The Resolved Logical plan will be passed on to a "Catalyst Optimizer" after it is generated. 

Catalyst Optimizer will try to optimize the plan after applying its own rule. 

Basically, the Catalyst Optimizer is responsible to perform logical optimization.

1) It checks all the tasks which can be performed and computed together in one Stage.

2) It decides the order of execution of queries for better performance in the case of a multi-join query.

3) It tries to optimize the query by evaluating the filter clause before any project.


# Spark Physical Plan

Physical Plan is an internal enhancement or optimization for Spark.

It is generated after creation of the Optimized Logical Plan .

What exactly does Physical Plan do?

Suppose, there’s a join query between two tables. In that join operation, one of them is a large table and the other one is a small table with a different number of partitions scattered in different nodes across the cluster (it can be in a single rack or a different rack). Spark decides which partitions should be joined at the start (order of joining), the type of join, etc. for better optimization.

Physical Plan is limited to Spark operation and for this, it will do an evaluation of multiple physical plans and finalize the suitable optimal physical plan. And ultimately, the finest Physical Plan runs.

Once the finest Physical Plan is selected, executable code (DAG of RDDs) for the query is created which needs to be executed in a distributed manner on the cluster.

This entire process is known as Codegen and that is the task of Spark’s Tungsten Execution Engine.

Physical plan is:

1. A bridge between Logical Plans and RDDs 
2. It is a tree 
3. Contains more specific description of how things (execution) should happen (specific choice of algorithm)
4. User lower-level primitives (RDDs)



explain(mode=”simple”) shows physical plan.

explain(mode=”extended”) presents physical and logical plans.

explain(mode=”codegen”) shows the java code planned to be executed.

explain(mode=”cost”) presents the optimized logical plan and related statistics (if they exist).

explain(mode=”formatted”) shows a split output created by an optimized physical plan outline, and a section of every node detail.


# SparkPlan

SparkPlan is a recursive data structure in Spark SQL’s Catalyst tree manipulation framework and as such represents a single physical operator in a physical execution query plan as well as a physical execution query plan itself (i.e. a tree of physical operators in a query plan of a structured query).

SparkPlan is the contract of physical operators to build a physical query plan (aka query execution plan).

A SparkPlan physical operator is a Catalyst tree node that may have zero or more child physical operators.

A structured query is basically a single SparkPlan physical operator with child physical operators.

The result of executing a SparkPlan is an RDD of internal binary rows, i.e. RDD[InternalRow].

Executing a structured query is simply a translation of the higher-level Dataset-based description to an RDD-based runtime representation that Spark will in the end execute

SparkPlan has access to the owning SparkContext (from the Spark Core).

**The entry point to Physical Operator Execution Pipeline is execute.**



