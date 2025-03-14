/*

Spark 2.0 ships with the second generation Tungsten engine.

This engine builds upon ideas from modern compilers and MPP databases and applies them to Spark workloads.

The main idea is to emit optimized code at runtime that collapses the entire query into a single function, eliminating virtual function calls 
and leveraging CPU registers for intermediate data.

We call this technique “whole-stage code generation.”
 
 */
package spark.engine;

public class Engine {

}
