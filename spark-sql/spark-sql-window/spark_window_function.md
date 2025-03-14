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

#### ranking function syntax

    RANK | DENSE_RANK | PERCENT_RANK | NTILE | ROW_NUMBER

### Analytic Functions

#### analytical function syntax

    CUME_DIST | LAG | LEAD | NTH_VALUE | FIRST_VALUE | LAST_VALUE

### Aggregate Functions

#### aggregate function syntax

    MAX | MIN | COUNT | SUM | AVG | ...

## nulls_option

Specifies whether to skip null values when evaluating the window function.

RESPECT NULLS means not skipping null values, while IGNORE NULLS means skipping.

If not specified, the default is RESPECT NULLS.

### null option syntax

    { IGNORE | RESPECT } NULLS

Note: `Only LAG | LEAD | NTH_VALUE | FIRST_VALUE | LAST_VALUE can be used with IGNORE NULLS`.

## window_frame

Specifies which row to start the window on and where to end it.

### frame syntax

    { RANGE | ROWS } { frame_start | BETWEEN frame_start AND frame_end }

`frame_start` and `frame_end` have the following syntax:

### offset Syntax

    UNBOUNDED PRECEDING | offset PRECEDING | CURRENT ROW | offset FOLLOWING | UNBOUNDED FOLLOWING

`offset`: specifies the offset from the position of the current row.

Note: If `frame_end` is omitted it defaults to `CURRENT ROW`.
