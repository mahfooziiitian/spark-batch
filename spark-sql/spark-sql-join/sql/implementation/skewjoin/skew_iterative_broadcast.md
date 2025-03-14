# Iterative Broadcast technique

It is an adaption of `Broadcast Hash` join in order to handle larger skewed datasets.

It is useful in situations where either of the input dataset cannot be broadcasted to executors.

This may happen due to the constraints on the executor memory limits.
 \In order to deal with such scenarios, ‘Iterative Broadcast’ technique breaks downs one of the input data set (preferably the smaller one) into one or more smaller chunks thereby ensuring that each of the resulting chunk can be easily broadcasted. These smaller chunks are then joined one by one with the other unbroken input dataset using the standard ‘Broadcast Hash’ Join. Outputs from these multiple joins is finally combined together using the ‘Union’ operator to produce the final output.

 One of the ways in which a Dataset can be broken into smaller chunks is to assign a random number out of the desired number of chunks to each record of the Dataset in a newly added column, ‘chunkId’. Once this new column is ready, a for loop is initiated to iterate on chunk numbers. For each iteration , firstly the records are filtered on the ‘chunkId’ column corresponding to current iteration chunk number. The filtered dataset, in each iteration, is then joined with the unbroken other input dataset using the standard ‘Broadcast Hash’ Join to get the partial joined output. The partial joined output is then combined with the previous partial joined output. After the loop is exited, one would get the overall output of the join operation of the two original datasets.

 However, in contrast to ‘Broadcast Hash Join’, ‘Iterative Broadcast Join’ is limited to ‘Inner Joins’ only. It cannot handle Full Outer Joins, Left Joins and Right Joins. However, for ‘Inner Joins’, it can handle skewness on both the datasets.
