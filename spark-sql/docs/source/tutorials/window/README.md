# Window function

Window functions in Spark SQL (and Databricks) are incredibly powerful tools that let you perform calculations across rows related to the current row, without collapsing them into a group like GROUP BY would.

Window functions operate on a group of rows, referred to as a `window`, and calculate a return value for `each row based` on the group of rows.

Window functions are useful for processing tasks such as calculating a `moving average`, `computing a cumulative statistic`, or `accessing the value of rows given the relative position` of the current row.

## Syntax

```text
window_function [ nulls_option ] OVER
( [  
    { PARTITION | DISTRIBUTE } BY partition_col_name = partition_col_val ( [ , ... ] ) ]
    { ORDER | SORT } BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [ , ... ]
    [ window_frame ] 
)
```
