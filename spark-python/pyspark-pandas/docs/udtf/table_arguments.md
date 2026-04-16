# UDTF Table Arguments

Pass an **entire table** as input to a UDTF using the `TABLE()` syntax.
Each row arrives as a `pyspark.sql.Row` object.

## Pattern

```python
from pyspark import Row
from pyspark.sql.functions import udtf

@udtf(returnType="id: int, label: string")
class FilterAndLabel:
    def eval(self, row: Row):
        if row["id"] > 5:
            yield row["id"], "above_threshold"
```

## SQL Usage

```sql
SELECT * FROM filter_and_label(TABLE(SELECT * FROM range(10)));
```

!!! note "Single table argument"
    By default only **one** `TABLE` argument is allowed per UDTF call. Enable
    `spark.sql.tvf.allowMultipleTableArguments.enabled` for multiple tables.

## Full Example

```python title="src/spp/udtf/udtf_table_argument.py"
--8<-- "src/spp/udtf/udtf_table_argument.py"
```

### Run

```bash
python src/spp/udtf/udtf_table_argument.py
```
