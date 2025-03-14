Spark SQL introduced an optimizer called Catalyst based on the functional programming of Scala.

Its two primary purposes are: 

1. To add new optimization techniques to solve some problems with "big data" 
2. To allow developers to expand and customize the optimizer functions.

The Catalyst Optimizer in Spark offers 

1. rule-based optimization 

Rule-based optimization indicates how to execute the query from a set of defined rules.

2. cost-based optimization

Cost-based optimization generates multiple execution plans and compares them to choose the lowest cost one.

Catalyst is written in Scala and bases on functional programming constructs.

# Tree

The main data type in Catalyst is the tree. Each tree is composed of nodes, and each node has a nodetype and zero or more children. These objects are immutable and can be manipulated with functional language

# Using Catalyst in Spark SQL

Phases
The four phases of the transformation that Catalyst performs are as follows:

1. Analysis
   The first phase of Spark SQL optimization is the analysis. Spark SQL starts with a relationship to be processed that can be in two ways. A serious form from an AST (abstract syntax tree) returned by an SQL parser, and on the other hand from a DataFrame object of the Spark SQL API.

2. Logic Optimization Plan
   The second phase is the logical optimization plan. In this phase, rule-based optimization is applied to the logical plan. It is possible to easily add new rules.

3. Physical plan
   In the physical plan phase, Spark SQL takes the logical plan and generates one or more physical plans using the physical operators that match the Spark execution engine. The plan to be executed is selected using the cost-based model (comparison between model costs).

4. Code generation
   Code generation is the final phase of optimizing Spark SQL. To run on each machine, it is necessary to generate Java code bytecode.


# 



