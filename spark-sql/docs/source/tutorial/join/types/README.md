# Introduction

## Join Types in Spark SQL

In Spark SQL, **joins** are used to combine rows from two or more DataFrames based on a related column. Understanding different join types is essential for effective data analysis.

### Common Join Types

- **Inner Join**: Returns rows with matching values in both DataFrames.
- **Left Outer Join**: Returns all rows from the left DataFrame, and matched rows from the right DataFrame.
- **Right Outer Join**: Returns all rows from the right DataFrame, and matched rows from the left DataFrame.
- **Full Outer Join**: Returns all rows when there is a match in either DataFrame.
- **Cross Join**: Returns the Cartesian product of both DataFrames.
- **Semi Join**: Returns rows from the left DataFrame where a match exists in the right DataFrame.
- **Anti Join**: Returns rows from the left DataFrame where no match exists in the right DataFrame.

---

> 💡 **Tip:** Choosing the right join type can optimize your queries and improve performance.

Explore the following sections to learn more about each join type with examples and best practices.
