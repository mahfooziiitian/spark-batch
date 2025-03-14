Checkpoint truncates the execution plan and saves the checkpoint data frame to a temporary 
location on the disk and reload it back in, which would be redundant anywhere else besides Spark. 

However, in Spark, it comes up as a performance-boosting factor. 

The point is that each time you apply a transformation or perform a query on a data frame, the query plan grows. 

Spark keeps all history of transformations applied on a data frame that can be seen when run explain command 
on the data frame. 

When the query plan starts to be huge, the performance decreases dramatically, generating bottlenecks.

Checkpoint helps to refresh the query plan and to materialize the data.

It is ideal for scenarios including iterative algorithms and branching out a new data frame to 
perform different kinds of analytics. 

More tangibly, after checkpointing the data frame, you don't need to recalculate all the 
previous transformations applied on the data frame, it is stored on disk forever. 

Note that, Spark won’t clean up the checkpoint data even after the sparkContext is destroyed 
and the clean-ups need to be managed by the application. 

It is also a good property of checkpointing to debug the data pipeline by checking the status 
of data frames.

Checkpointing is a process of writing received records (by means of input dstreams) at checkpoint intervals to a highly-available HDFS-compatible storage. It allows creating fault-tolerant stream processing pipelines so when a failure occurs input dstreams can restore the before-failure streaming state and continue stream processing (as if nothing had happened).

Checkpointing, through checkpoint(), to save your data, without the lineage.

The checkpoint() method will truncate the DAG (or logical plan) and save the content of the dataframe to disk.

When eager, the checkpoint will be created right away. If you use false, with the checkpoint() method, the checkpoint will be created when an action is called. 

if you do not put a cache or checkpoint after the filter, the filter will be recomputed every time.
