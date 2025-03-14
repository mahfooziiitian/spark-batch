# Window function

Window functions operate on a group of rows, referred to as a `window`, and calculate a return value for `each row based` on the group of rows.

Window functions are useful for processing tasks such as calculating a `moving average`, `computing a cumulative statistic`, or `accessing the value of rows given the relative position` of the current row.

## Syntax 

    window_function [ nulls_option ] OVER
    ( [  { PARTITION | DISTRIBUTE } BY partition_col_name = partition_col_val ( [ , ... ] ) ]
      { ORDER | SORT } BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [ , ... ]
      [ window_frame ] )


## window_function

### Ranking Functions

#### Syntax: 

    RANK | DENSE_RANK | PERCENT_RANK | NTILE | ROW_NUMBER

### Analytic Functions

#### Syntax: 

    CUME_DIST | LAG | LEAD | NTH_VALUE | FIRST_VALUE | LAST_VALUE

### Aggregate Functions

#### Syntax: 

    MAX | MIN | COUNT | SUM | AVG 

