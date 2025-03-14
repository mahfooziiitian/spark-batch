If you are familiar with Hadoop, a Worker Node is something similar to a slave node.

Worker machines are the machines where the actual work is happening in terms of execution inside Spark executors.

This process reports the available resources on the node to the master node. Typically each node in a Spark cluster except the master runs a worker process.

We normally start one spark worker daemon per worker node, which then starts and monitors executors for the applications.

