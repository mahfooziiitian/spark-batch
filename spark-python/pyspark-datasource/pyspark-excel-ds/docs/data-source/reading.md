# Reading Excel

`ExcelReader` provides a fluent, immutable API over `pandas.read_excel()`.

## Basic read

```python
from pys_excel import ExcelReader, get_spark

spark = get_spark("read-demo")
df = ExcelReader(spark).sheet("Employees").read("data/employees.xlsx")
```

- `sheet(name_or_index)` selects a worksheet by name or zero-based index.
  Defaults to the first sheet (`0`) if not set.
- `read(path)` returns a single-sheet DataFrame.

## Reading every sheet

```python
sheets = ExcelReader(spark).read_all_sheets("data/employees.xlsx")
# {"Employees": DataFrame, "Departments": DataFrame}
```

## Fluent modifiers

| Method | Purpose |
|--------|---------|
| `sheet(name_or_index)` | Select a sheet |
| `header(row)` | Header row index (0-based); `None` for headerless sheets |
| `skiprows(n)` | Skip `n` leading rows before the header |
| `nrows(n)` | Limit the number of data rows read |
| `usecols(cols)` | Restrict columns — Excel range string (`"A:C"`) or a list |
| `names(cols)` | Override column names (pair with `header(None)`) |
| `na_values(values)` | Extra strings treated as null |
| `dtype(mapping)` | Force pandas dtypes for specific columns |
| `keep_default_na(bool)` | Toggle pandas' built-in NA recognition |
| `engine(name)` | pandas Excel engine (`openpyxl` default, `xlrd` for legacy `.xls`) |
| `with_schema(schema)` | Apply an explicit Spark `StructType` or DDL string |
| `with_option(key, value)` / `with_options(**kwargs)` | Escape hatch for any `read_excel` option |

All modifiers return a **new** `ExcelReader` instance — the original is never mutated:

```python
base = ExcelReader(spark)
employees = base.sheet("Employees").header(0)
departments = base.sheet("Departments")  # independent of `employees`
```

## Explicit schema

```python
schema = "emp_id STRING, name STRING, department STRING, salary DOUBLE"
df = ExcelReader(spark).sheet("Employees").with_schema(schema).read("data/employees.xlsx")
```

Internally, the DataFrame is created with inferred types and then `cast()` to
your schema column-by-column — this avoids type-mismatch errors you'd get from
handing pandas' inferred types straight to `createDataFrame(..., schema=...)`
(e.g. a whole-number salary column inferred as `int` failing against a
`DoubleType` field).

See [Schema](../schema/index.md) for more on why this matters with Excel data.
