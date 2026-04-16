# Arrow-Optimized UDTF

Arrow UDTFs use `eval()` with `useArrow=True` to transfer data in columnar
Arrow batches, providing better throughput for large result sets.

## Class Structure

```python
from pyspark.sql.functions import udtf

@udtf(returnType="id: int, value: double", useArrow=True)   # (1)!
class ArrowRangeUDTF:
    def eval(self, n: int):                                  # (2)!
        for i in range(n):
            yield (i, float(i) * 1.5)
```

1. `useArrow=True` enables Arrow-based serialization for input and output.
2. `eval()` works the same way — yield tuples. Arrow handles the serialization.

## Arrow vs Non-Arrow

| Aspect | Standard UDTF | Arrow UDTF |
|--------|--------------|------------|
| Serialization | Pickle (row-by-row) | Arrow (columnar batches) |
| Throughput | Baseline | **Higher for large outputs** |
| Data types | All Python types | Arrow-compatible types |
| Spark version | 3.5+ | 3.5+ |

## When to Use

!!! success "Good fit"

    - Generating large numbers of rows (10K+)
    - Numerical data (int, float, double)
    - Performance-critical table functions

!!! failure "Not a good fit"

    - Small result sets (< 1000 rows) — overhead not worth it
    - Complex nested types not yet supported by Arrow

## Usage

```python
spark.udtf.register("arrow_range", ArrowRangeUDTF)
spark.sql("SELECT * FROM arrow_range(10)").show()
```

## Output

```
+---+-----+
| id|value|
+---+-----+
|  0|  0.0|
|  1|  1.5|
|  2|  3.0|
|  3|  4.5|
|  4|  6.0|
|  5|  7.5|
|  6|  9.0|
|  7| 10.5|
|  8| 12.0|
|  9| 13.5|
+---+-----+
```

!!! note

    Arrow UDTFs require PySpark 3.5+ and `pyarrow` installed on all workers.
