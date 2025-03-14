# Project Tungsten

Spark Dataset/DataFrame includes Project Tungsten which optimizes Spark jobs for Memory and CPU efficiency.

Tungsten is a Spark SQL component that provides increased performance by 
rewriting Spark operations in bytecode, at runtime.

Tungsten performance by focusing on jobs close to bare metal CPU and memory efficiency.

Since DataFrame is a column format that contains additional metadata, 
hence Spark can perform certain optimizations on a query.

Before your query is run, a logical plan is created using Catalyst Optimizer and then it’s executed using the Tungsten execution engine.


