# Tungsten

Project Tungsten was created to improve the efficiency of using memory and
CPU in Spark applications and to push the performance closer to the limits of modern
hardware.

There are three initiatives in the Tungsten project.

• `Off-heap management`: Manage memory explicitly by using `off-heap management` techniques to eliminate the overhead of the JVM object model and minimize garbage collection.

• `Memory hierarchy`: Use `intelligent cache-aware` algorithms and data structures to exploit memory hierarchy.

• `Whole-stage code generation`: Use whole-stage code generation to minimize virtual function calls by combining multiple operators into a single Java function.

Much of the work in the Tungsten project happens behind the scenes in the execution engine. 


