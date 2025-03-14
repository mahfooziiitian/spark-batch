"""
There are five types of boundaries, which are

1. UNBOUNDED PRECEDING
2. UNBOUNDED FOLLOWING
3. CURRENT ROW
4. PRECEDING
4. FOLLOWING

UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING represent the first row of the partition and the last row of the
partition, respectively.

"""

from pyspark.sql.window import Window

if __name__ == '__main__':
    windowSpec = (Window
                  .partitionBy(...)
                  .orderBy(...))
