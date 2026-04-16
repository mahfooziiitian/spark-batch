# Pandas-on-Spark Options

Control display and compute behaviour using `ps.get_option()`,
`ps.set_option()`, `ps.reset_option()`, and `ps.option_context()`.

## Key Options

| Option | Default | Description |
|--------|---------|-------------|
| `display.max_rows` | `1000` | Max rows to display |
| `compute.max_rows` | `1000` | Max rows for operations like `value_counts` |
| `compute.ops_on_diff_frames` | `False` | Allow cross-frame operations |
| `compute.shortcut_limit` | `1000` | Row limit for shortcut optimizations |

## Temporary Override with `option_context`

```python
with ps.option_context("display.max_rows", 5, "compute.max_rows", 500):
    # options are active only inside this block
    print(ps.get_option("display.max_rows"))  # 5
# options revert here
```

## Full Example

```python title="src/spp/pandas_on_spark/pandas_on_spark_options.py"
--8<-- "src/spp/pandas_on_spark/pandas_on_spark_options.py"
```

### Run

```bash
python src/spp/pandas_on_spark/pandas_on_spark_options.py
```
