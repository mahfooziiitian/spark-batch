# String Functions

Transform, extract, and validate string column values.

## Function Reference

| Function | Purpose | Example |
|----------|---------|---------|
| `F.upper(col)` | Uppercase | `F.upper("name")` |
| `F.lower(col)` | Lowercase | `F.lower("name")` |
| `F.trim(col)` | Strip leading and trailing whitespace | `F.trim("name")` |
| `F.ltrim / rtrim` | Strip left / right whitespace | |
| `F.length(col)` | Character count | `F.length("name")` |
| `F.substring(col, pos, len)` | Extract substring (1-indexed) | `F.substring("name", 1, 3)` |
| `F.concat(*cols)` | Concatenate strings | `F.concat("first", "last")` |
| `F.concat_ws(sep, *cols)` | Concatenate with separator | `F.concat_ws(" ", "first", "last")` |
| `F.split(col, pat)` | Split string into array | `F.split("tags", ",")` |
| `F.regexp_extract(col, pat, idx)` | Extract regex group | `F.regexp_extract("email", r"@(.+)", 1)` |
| `F.regexp_replace(col, pat, rep)` | Replace regex matches | `F.regexp_replace("text", r"\s+", " ")` |
| `F.like(col, pat)` | SQL LIKE pattern match | `F.col("name").like("A%")` |
| `F.rlike(col, pat)` | Regex match → boolean | `F.col("email").rlike(r".+@.+\..+")` |
| `F.initcap(col)` | Title-case each word | `F.initcap("name")` |
| `F.lpad / rpad` | Pad string to fixed width | `F.lpad("id", 5, "0")` |
| `F.format_string(fmt, *cols)` | `sprintf`-style formatting | `F.format_string("%.2f", "rev")` |
| `F.translate(col, matching, replace)` | Character-level replace | |

## Example

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("string-functions")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

data = [
    (1, "  Alice Smith  ", "alice@example.com",  "python,spark,scala"),
    (2, "bob jones",       "BOB@EXAMPLE.COM",    "java,spark"),
]
df = spark.createDataFrame(data, ["id", "full_name", "email", "tags"])

result = (df
          .withColumn("name_clean",   F.initcap(F.trim("full_name")))             # (1)!
          .withColumn("email_lower",  F.lower("email"))                           # (2)!
          .withColumn("domain",       F.regexp_extract("email", r"@(.+)", 1))     # (3)!
          .withColumn("email_valid",  F.col("email").rlike(r".+@.+\..+"))         # (4)!
          .withColumn("first_name",   F.split(F.trim("full_name"), r"\s+")[0])    # (5)!
          .withColumn("tag_array",    F.split("tags", ",")))                      # (6)!
result.show(truncate=False)
```
1. `trim` removes whitespace, then `initcap` title-cases each word.
2. Normalise emails to lowercase before storing or comparing.
3. Capture group 1 from the regex — everything after `@`.
4. Returns `true`/`false` — useful as a data quality flag.
5. Array indexing (0-based) extracts the first element.
6. Splits a delimited string into an `ArrayType(StringType)` column.

### Run

```bash
python src/data_frame/columns/column_operation.py
```

## Concatenate with Separator

```python
df = df.withColumn(
    "full_label",
    F.concat_ws(" | ", F.col("region"), F.col("category"), F.col("status"))
)
```

## Replace with Regex

```python
# Collapse multiple spaces to one
df = df.withColumn("clean_text", F.regexp_replace("text", r"\s+", " "))

# Mask credit card numbers
df = df.withColumn("masked_cc", F.regexp_replace("cc_number", r"\d(?=\d{4})", "*"))
```

## Pad / Format

```python
df = df.withColumn("padded_id",     F.lpad(F.col("id").cast("string"), 8, "0"))
df = df.withColumn("revenue_fmt",   F.format_string("$%.2f", F.col("revenue")))
```

!!! tip "Use F.col().rlike() for boolean flag columns"
    `F.col("email").rlike(pattern)` returns a `BooleanType` column — perfect for
    data quality reports or `filter()` conditions.
