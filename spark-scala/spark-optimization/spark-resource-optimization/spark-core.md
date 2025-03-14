Spark job will have many stages, and within each stage there will be many tasks.

Spark will at best schedule a thread per task per core, and each task will process a distinct partition.

To optimize resource utilization and maximize parallelism, the ideal is at least as many partitions as there are cores on the executor.
If there are more partitions than there are cores on each executor, all the cores are kept busy. You can think of partitions as atomic units of parallelism: a single thread running on a single core can work on a single partition.
