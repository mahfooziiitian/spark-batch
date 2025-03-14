# Rollup

When working with hierarchical data such as the revenue data that spans different departments and divisions, rollup can
easily calculate the subtotals
and a grand total across them.

Rollup respects the given hierarchy of the given set of rollup columns and always starts the rolling-up process with the
first column in the hierarchy.

The grand total is listed in the output where all the column values are null.

# Cube

A cube is basically a more advanced version of a rollup.
It performs the aggregations across all the combinations of the grouping columns.
Therefore, the result includes what a rollup provides as well as other combinations.
In our example of cubing by the origin_state and origin_city, the result will include the aggregation for each of the
original cities.

1. Total across all dates and countries
2. Total for each county across all dates
3. Total for across all dates for each country
4. Total for each date for each country

# Pivot

Pivot makes it possible for us to convert row into column.

The number of columns added after the group columns in the result table is the product of the number of unique values of
the pivot column and the number of aggregations.

If the pivoting column has a lot of distinct values, you can selectively choose which
values to generate the aggregations for.

Specifying a list of distinct values for the pivot column actually will speed up the
pivoting process. Otherwise, Spark will spend some effort in figuring out a list of distinct
values on its own.