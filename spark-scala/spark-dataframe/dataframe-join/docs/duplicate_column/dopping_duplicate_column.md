# Doping duplicate column

## Dropping duplicate column after join

Another approach is to drop the offending column after the join.
When doing this, we need to refer to the column via the original source DataFrame.
We can do this if the join uses the same key names or if the source DataFrames have columns that simply have the same name.

## Same column name

When you have two keys that have the same name, probably the easiest fix is to change the join
expression from a Boolean expression to a string or sequence.

    val dupNameDF = employeeDF.join(deptDF2, "dept_no")

This automatically removes one of the columns for you during the join.

## Rename the column before join
