# UDTF SQL Integration

Register UDTFs for use in Spark SQL queries, including `LATERAL` joins.

## Register a UDTF

```python
spark.udtf.register("split_words", WordSplitter)
```

## Use in SQL

```sql
-- Basic call
SELECT * FROM split_words('hello world');

-- LATERAL join with table data
SELECT t.text, w.word
FROM VALUES ('Apache Spark'), ('Pandas on Spark') AS t(text),
LATERAL split_words(t.text) w;
```

## Full Example

```python title="src/spp/udtf/udtf_sql.py"
--8<-- "src/spp/udtf/udtf_sql.py"
```

### Run

```bash
python src/spp/udtf/udtf_sql.py
```
