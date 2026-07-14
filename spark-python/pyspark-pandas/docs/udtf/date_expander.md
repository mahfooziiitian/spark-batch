# Date Expander UDTF

A practical UDTF that expands a `(start_date, end_date)` pair into one row
per calendar day — useful for generating time-series scaffolding.

## Usage

```python
DateExpander(lit("2024-01-28"), lit("2024-02-02")).show()
```

Output:

```text
+----------+
|      date|
+----------+
|2024-01-28|
|2024-01-29|
|2024-01-30|
|2024-01-31|
|2024-02-01|
|2024-02-02|
+----------+
```

## Full Example

```python title="src/spp/udtf/data_expander.py"
--8<-- "src/spp/udtf/data_expander.py"
```

### Run

```bash
python src/spp/udtf/data_expander.py
```
