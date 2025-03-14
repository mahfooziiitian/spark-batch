/*

When a Spark application is run:

    a) Spark connects to a cluster manager and acquires executors on the worker nodes.

    b) As mentioned earlier, a Spark application submits a data processing algorithm as a job.

    c) Spark splits a job into a directed acyclic graph (DAG) of stages.

    d) It then schedules the execution of these stages on the executors using a low-level scheduler provided by a cluster manager.

    e) The executors run the tasks submitted by Spark in parallel.

    f)  Every Spark application gets its own set of executors on the worker nodes.


 */
package spark.working;

public class Working {
}
