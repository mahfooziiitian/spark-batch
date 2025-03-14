/*
Some time ago, Spark introduced Code Generation for expression evaluation in SQL and data frames. Expression evaluation is the process of computing the value of an expression on a particular record. At runtime, Spark dynamically generates byte code for evaluating these expressions rather than stepping through a slower interpreter for each role. Compared with interpretation, Code generation reduces the boxing of primitive data types, and more importantly, it avoids expensive polymorphic function dispatches.

Generic evaluation of expression logic is very expensive on the JVM:
Virtual function calls
Branches based on expression type
Object creation due to primitive boxing
Memory consumption by boxed primitive objects
Generating custom bytecode can eliminate these overheads


 */
package com.mahfooz.spark.dataset.optimization.tungusten.codegeneration

object CodeGeneration {

}
