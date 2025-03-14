# Left join

## Left Semi join

The behavior of this join type is similar to the inner join type, except the joined dataset
does not include the columns from the right dataset.
Another way of thinking about this join type is its behavior is the opposite of the left anti-join, where the joined
dataset contains only the matching rows.
Semi-joins are a bit of a departure from the other joins.
They do not actually include any values from the right DataFrame.
They only compare values to see if the value exists in the second DataFrame.
If the value does exist, those rows will be kept in the result, even if there are duplicate keys in the left DataFrame.

## Left anti-join

Left anti joins are the opposite of left semi joins.
Like left semi joins, they do not actually include any values from the right DataFrame.
They only compare values to see if the value exists in the second DataFrame.
However, rather than keeping the values that exist in the second DataFrame, they keep only the
values that do not have a corresponding key in the second DataFrame.
Think of anti joins as a NOT IN SQL-style filter.
This join type enables you to find out which rows from the left dataset don't have any matching rows on the right
dataset, and the joined dataset will contain only the columns from the left dataset.
The result from the left anti-join can easily tell you which employees are not assigned to a department.
Notice the right anti-join type doesn't exist; however, you can easily switch the datasets around to achieve the same
goal.

