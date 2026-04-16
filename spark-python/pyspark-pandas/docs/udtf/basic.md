# Basic UDTF

Create UDTFs using the `@udtf` decorator or the `udtf()` function.

## Decorator Pattern

```python
from pyspark.sql.functions import udtf

@udtf(returnType="num: int, squared: int")
class SquareNumbers:
    def eval(self, start: int, end: int):
        for num in range(start, end + 1):
            yield num, num * num
```

## Function Registration Pattern

```python
class SquareNumbers:
    def eval(self, start: int, end: int):
        for num in range(start, end + 1):
            yield num, num * num

square_num = udtf(SquareNumbers, returnType="num: int, squared: int")
square_num(lit(1), lit(5)).show()
```

## Full Example

```python title="src/spp/udtf/python_udtf.py"
--8<-- "src/spp/udtf/python_udtf.py"
```

### Run

```bash
python src/spp/udtf/python_udtf.py
```
