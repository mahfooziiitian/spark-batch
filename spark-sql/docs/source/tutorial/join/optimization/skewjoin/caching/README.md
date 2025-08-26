
# Skew Join Optimization Using Caching

When dealing with skewed joins in Spark, caching can significantly improve performance:

- **Cache the Smaller Table:**  
    If one side of the join is much smaller and fits in memory, cache or broadcast it. This reduces unnecessary shuffling and speeds up the join operation.

- **How to Cache:**  
    Use the following Spark command to cache a DataFrame:

    ```python
    small_df.cache()
    ```

- **Broadcast Join Example:**  
    For very small tables, broadcasting is even more efficient:

    ```python
    from pyspark.sql.functions import broadcast

    result = large_df.join(broadcast(small_df), "key")
    ```

> **Tip:** Always monitor memory usage to ensure caching does not cause spills to disk.
