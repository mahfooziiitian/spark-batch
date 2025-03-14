Each Spark executor is a JVM process and is exclusively allocated to a specific Spark application.
This was a conscious design decision to avoid sharing a Spark executor between multiple Spark applications in order to isolate them from each other so one badly behaving Spark application wouldn’t affect other Spark applications.

The lifetime of a Spark executor is the duration of a Spark application, which could run for a few minutes or for a few days. Since Spark applications are running in separate Spark executors, sharing data between them will require writing the data to an external storage system like HDFS.

Spark driver is the master and the Spark executor is the slave.

Each of these components runs as an independent process on a Spark cluster. A Spark application consists of one and only one Spark driver and one or more Spark executors. Playing

Playing the slave role, each Spark executor does what it is told, which is to execute the data processing logic in the form of tasks. Each task is executed on a separate CPU core. This is how Spark can speed up the processing of a large amount of data by processing it in parallel. In addition to executing assigned tasks, each Spark executor has the responsibility of caching a portion of the data in memory and/or on disk when it is told to do so by the application logic.

The master allocates the resources and uses the workers running across the cluster to create Executors for the driver. The driver can then use these executors to run its tasks. 

Executors are only launched when a job execution starts on a worker node.

Each application has its own executor processes, which can stay up for the duration of the application and run tasks in multiple threads. This also leads to the side effect of application isolation and non-sharing of data between multiple applications. Executors are responsible for running tasks and keeping the data in memory or disk storage across them.