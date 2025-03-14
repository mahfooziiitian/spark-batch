# Iterative query performance improvement process

Here is a high-level iterative process to improve query performance:

1. List business-critical queries, the most frequently run queries, and the slowest queries.
2. Check the query plans for each of these queries using the EXPLAIN keyword and see the amount of data being used at each stage (we will be learning about how to view query plans in the later chapters).
3. Identify the joins or filters that are taking the most time. Identify the corresponding data partitions.
4. Try to split the corresponding input data partitions into smaller partitions, or change the application logic to perform isolated processing on top of each partition and later merge only the filtered data.
5. You could also try to see if other partitioning keys would work better and if you need to repartition the data to get better job performance for each partition.
6. If any particular partitioning technology doesn't work, you can explore having more than one piece of partitioning logic—for example, you could apply horizontal partitioning within functional partitioning, and so on.
7. Monitor the partitioning regularly to check if the application access patterns are balanced and well distributed. Try to identify hot spots early on.
8. Iterate this process until you hit the preferred query execution time.