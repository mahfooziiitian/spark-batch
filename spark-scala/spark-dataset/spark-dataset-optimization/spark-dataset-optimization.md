Project Tungsten improves Apache Spark's execution engine that concentrates on enhancing memory and CPU's efficiency for Spark applications to push performance closer to its current hardware limitations. Project Tungsten is a part responsible for the code generation, which has such main initiatives.

# Memory management

Spark is not just a general-purpose application. 
It understands how data flows through various stages of computation and the scope of jobs and tasks. As a result, Spark knows much more information about the life cycle of memory blocks than the JVM garbage collector. Thus, it should be able to manage memory more efficiently. To tackle the memory management problem, Tungsten is introduced as an explicit memory manager to convert most Spark operations to operate directly against binary data rather than Java objects. This builds on Sun.Misc.Unsafe and advanced functionality provided by the JVM that exposes C-style memory access.
It leverages application semantics to manage memory explicitly and eliminate the overhead of JVM object model and garbage collection.

Spark offers:

Explicit memory management (builds on sun.misc.Unsafe)
Processing directly against binary data
Example of an UnsafeRow: ( 2018, "EPAM", "Big Data")

# Binary memory model

It has its own memory model.
It works with binary data.


# Driver memory
# Increasing Memory

    spark-shell --driver-memory 8g
    spark-shell --executor-memory 8g

# Scaling Driver
 

# Memory usage in spark
Memory usage in Spark falls into two large categories, 

1. execution memory
2. storage memory

In Spark, execution and storage share a unified region.
When no execution memory is utilized, storage can acquire all the available memory and vice versa.

There are specific strategies that you can take to use memory in your application more efficiently.

1. Java heap reserved memory

1.1 User Memory

    It can store your own data structures there that would be used in RDD transformations.

    spark.memory.fraction

1.2 Spark memory
    
    spark.memory.fraction=
    
a) storage memory

    It is used for caching and propagating internal data across the Cluster.

    spark.memory.storagefraction=

b) execution memory

    It is used for computation in shuffles, joins, sorts and aggregations.
    
    

2. Reserved memory

    
    It is used for internal needs.
    


# Executor Memory of offHeap

The benefits of this kind of memory are that objects are serialized to a byte array, managed by the operating system, but stored outside the process heap in native memory.

Not processed by the garbage collector, it is slower than on-heap storage memory but still faster than the reading/writing from a disk.

Below is an example how to turn on offHeap.

    spark.memory.offHeap.enabled = true
    spark.memory.offHeap.size = 3g

In Spark's shuffle subsystem, serialization and hashing (which are CPU bound) have been shown to be key bottlenecks, 
rather than raw network throughput of the underlying hardware.

# Code Generation

Using code generation to exploit modern compilers and CPUs.

# Cache-Aware Computation
Algorithms and data structures to exploit memory hierarchy.

# Leveraging speculation

Sometimes, when you face a problem, when all tasks are completed, and one task is running super slow, it can 
be due to cluster issues, connectivity issues with the resources on a particular node. 

In this case, you can enable speculative task execution.

Spark runs an additional task if suspects a task is running on the straggler node. 

    spark.speculation = true
    spark.speculation.interval = 100

# denotes rate to see if speculation is needed
    
    spark.speculation.multiplier = 1.5

# define how many times task hast to be slower than medial to trigger speculation
    
    spark.speculation.quantile = 0.95


# Level of parallelism

A cluster will not be fully utilized unless you set the level of parallelism for each operation high enough. Spark automatically sets the number of map tasks to run on each file according to its size, though, you can control it through operational parameters like the Spark context text file.
For distributed reduce operations such as groupByKey and reduceByKey, it uses the largest parent RDD number of partitions.
You can pass the level of parallelism as a second argument or set the spark.default.parralelizm config property to change the default.

#Configures the number of partitions to use when shuffling data for joins or aggregations.

    spark.sql.shuffle.partition = 200

# Default number of partitions in RDD after transformation like join reduceByKey, parallelize.

    spark.default.parralelizm = 10


In general, it's recommended to use 2-3 tasks per CPU core in your cluster. Roughly speaking, more partitions means more parallelism.

This can increase execution performance greatly. However, the optimal value depends on the Cluster, application and the data itself.
