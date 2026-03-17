# Shuffle Sort-Merge Join

The **Shuffle Sort-Merge Join** is a distributed join strategy in Apache Spark that efficiently joins large datasets by leveraging data shuffling and sorting.

---

## How It Works

1. **Shuffle Phase**  
    Data is shuffled so that rows with the same join key are sent to the same worker node (partition).

2. **Sort Phase**  
    Within each partition, both datasets are sorted by the join key.

3. **Merge Phase**  
    The sorted datasets are merged by iterating over both sides and joining rows with matching keys.

---

## Key Points

- **Default Strategy:**  
  Since Spark 2.3, Shuffle Sort-Merge Join is the default join strategy.  
  You can disable it by setting:  

  ```sql
  SET spark.sql.join.preferSortMergeJoin = false;
  ```

- **Supported Join Types:**  
  Works with all join types (`inner`, `left`, `right`, `outer`, etc.).

- **Join Condition:**  
  Only supports equality (`=`) joins.

- **Join Key Requirements:**  
  Join keys must be sortable.

---

## Example

```scala
val df1 = spark.table("table1")
val df2 = spark.table("table2")

val joined = df1.join(df2, df1("id") === df2("id"))
```

---

## References

- [Spark SQL Join Strategies](https://spark.apache.org/docs/latest/sql-performance-tuning.html#join-strategies)
- [Spark SQL Configuration](https://spark.apache.org/docs/latest/sql-ref-syntax.html#set-configuration-properties)
