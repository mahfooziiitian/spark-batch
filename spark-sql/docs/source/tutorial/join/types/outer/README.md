# Introduction

Outer joins in Spark SQL allow you to combine rows from two DataFrames based on a related column, including rows that do not have matching values in both tables.

---

## 🚀 What is an Outer Join?

An **outer join** returns all records when there is a match in either left or right DataFrame. There are three main types:

- **Left Outer Join**: Returns all rows from the left DataFrame, and matched rows from the right DataFrame.
- **Right Outer Join**: Returns all rows from the right DataFrame, and matched rows from the left DataFrame.
- **Full Outer Join**: Returns all rows when there is a match in one of the DataFrames.

---

## 💡 Example

```sql
SELECT *
FROM table1
FULL OUTER JOIN table2
ON table1.id = table2.id
```

---

## 📚 Learn More

- [Spark SQL Join Documentation](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html)
- [Databricks: Joins in Apache Spark](https://docs.databricks.com/en/learn/dataframes/joins.html)

---

Enhance your data analysis by mastering outer joins in Spark SQL!
