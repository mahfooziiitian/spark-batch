# Basic UDTF

The simplest UDTF pattern — implement `eval()` to yield rows.

## Class Structure

```python
from pyspark.sql.functions import udtf

@udtf(returnType="num: int, squared: int")     # (1)!
class SquareNumbers:
    def eval(self, n: int):                     # (2)!
        for i in range(1, n + 1):
            yield (i, i * i)
```

1. `returnType` declares the output schema as a DDL string.
2. `eval()` is called once per input row. Yield multiple rows.

## Usage

```python
# SQL registration
spark.udtf.register("square_numbers", SquareNumbers)
spark.sql("SELECT * FROM square_numbers(5)").show()

# DataFrame API
from pyspark.sql.functions import lit
SquareNumbers(lit(5)).show()
```

## Output

```
+---+-------+
|num|squared|
+---+-------+
|  1|      1|
|  2|      4|
|  3|      9|
|  4|     16|
|  5|     25|
+---+-------+
```

## When to Use

!!! success "Good fit"

    - Generating sequences or test data
    - Exploding complex structures into rows
    - Lookup tables from parameters

!!! failure "Not a good fit"

    - Stateful computation → use [Lifecycle UDTF](lifecycle-udtf.md)
    - High-throughput batch generation → use [Arrow UDTF](arrow-udtf.md)
