# Code Generation

The final phase of Spark  SQL optimization is code generation.

It involves generating Java bytecode to run on each machine. Catalyst uses the special feature of Scala language, "Quasiquotes" to make code generation easier because it is very tough to build code generation engines.

`Quasiquotes` lets the programmatic construction of abstract syntax trees (ASTs) in the Scala language, which can then be fed to the Scala compiler at runtime to generate bytecode.

With the help of a catalyst, we can transform a tree representing an expression in SQL to an AST for Scala code to evaluate that expression, and then compile and run the generated code.

Code Generation
Once the optimal physical plan has been chosen, the Catalyst will produce the code that will actually be used to interact with RDDs and execute the workload.

This phase can be seen as a stack: each transformation and action part of the workload will be sequentially added to the stack. Once all transformations and actions have been added, the code will be generated starting from the last one and then going backwards.

An interesting downside of this approach is tied to the language used to write Spark, Scala. Code generation is implemented via a recursion (note: not a tail recursion, just a plain recursion): as such, at each recursive invocation a new frame has to be allocated on the stack. Scala can allocate a finite number of frames. As a result, trying to optimize an extremely long query might result in the Driver crashing. While this occurance is pretty rare, it is still possible.
