"""
A window specification defines which rows are included in the frame associated with a given input row.

Partitioning Specification:

Controls which rows will be in the same partition with the given row.
Also, the user might want to make sure all rows having the same value for  the category column are collected to the same
machine before ordering and calculating the frame.
If no partitioning specification is given, then all data must be collected to a single machine.

Ordering Specification:

Controls the way that rows in a partition are ordered, determining the position of the given row in its partition.

Frame Specification:

States which rows will be included in the frame for the current input row, based on their relative position to the
current row.

"""

from pyspark.sql.window import Window

if __name__ == '__main__':
    windowSpec = (Window
                  .partitionBy(...)
                  .orderBy(...))
