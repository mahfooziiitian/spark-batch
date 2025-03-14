# Pivoting

It is a great way of transforming the table to create a different view, more suitable to doing many
summarizations and aggregations.

This is accomplished by taking the values of a column and making each of the values an actual column.

The `PIVOT` clause is used for data perspective.

We can get the aggregated values based on specific column values, which will be turned to multiple columns used in SELECT clause.

The PIVOT clause can be specified after the table name or subquery.

    PIVOT ( 
            { aggregate_expression [ AS aggregate_expression_alias ] }
            [ , ... ]
            FOR column_list IN ( expression_list ) 
    )

`aggregate_expression`

  Specifies an aggregate expression (SUM(a), COUNT(DISTINCT b), etc.).

`aggregate_expression_alias`

  Specifies an alias for the aggregate expression.

`column_list`

  Contains columns in the FROM clause, which specifies the columns we want to replace with new columns.
  We can use brackets to surround the columns, such as (c1, c2).

`expression_list`

  Specifies new columns, which are used to match values in column_list as the aggregating condition.

## Examples

### 1. Creating table

  CREATE TABLE students (
      id INT,
      name STRING,
      gender STRING,
      weight INT
  );

### 2. Insert into table

  INSERT INTO students (id, name, gender, weight) VALUES
  (1, 'Alice', 'F', 55),
  (2, 'Bob', 'M', 75),
  (3, 'Charlie', 'M', 85),
  (4, 'Diana', 'F', 65),
  (5, 'Eva', 'F', 70),
  (6, 'Frank', 'M', 90);

### 3. Pivot data

    SELECT * FROM students
    PIVOT (min(weight) AS min, 
            max(weight) AS max,
            avg(weight) AS avg
    FOR gender IN ('M' AS Male, 'F' AS Female))
