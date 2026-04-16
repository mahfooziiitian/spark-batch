# Stateful UDTF

Use `__init__` to set up state, `eval` to accumulate, and `terminate` to
emit final results — useful for custom aggregation patterns.

## Pattern

```python
@udtf(returnType="cnt: int")
class CountUDTF:
    def __init__(self):
        self.count = 0

    def eval(self, x: int):
        self.count += 1

    def terminate(self):
        yield (self.count,)
```

!!! note "Partition scope"
    State is **per partition**. With 2 partitions of 5 rows each, you get
    two rows of `cnt=5`, not one row of `cnt=10`.

## Full Example

```python title="src/spp/udtf/count_utdf.py"
--8<-- "src/spp/udtf/count_utdf.py"
```

### Run

```bash
python src/spp/udtf/count_utdf.py
```
