# Word Count

Classic MapReduce word count implemented with the PySpark DataFrame API.
A great first job to verify that your Spark environment is working correctly.

## What it does

1. Accepts text input from a file path, CLI argument, or a built-in sample
2. Splits each line on whitespace and lowercases every word
3. Counts occurrences with `groupBy` + `count()`
4. Writes the sorted results to CSV

## Run it

=== "Local (direct)"
    ```bash
    python spark-submit/word_count.py
    ```

=== "spark-submit local"
    ```bash
    spark-submit --master local[*] spark-submit/word_count.py
    ```

=== "Custom input file"
    ```bash
    INPUT_FILE=/path/to/file.txt spark-submit --master local[*] spark-submit/word_count.py
    ```

=== "YARN"
    ```bash
    spark-submit \
      --master yarn \
      --deploy-mode cluster \
      spark-submit/word_count.py
    ```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `INPUT_FILE` | built-in sample | Path to input text file |
| `OUTPUT_PATH` | `/tmp/word_count_output` | Destination directory for CSV output |

## Source

```python title="spark-submit/word_count.py"
--8<-- "spark-submit/word_count.py"
```

## Key steps explained

| Step | Code | What it does |
|------|------|-------------|
| Split | `F.split(col("value"), r"\s+")` | Tokenise each line on whitespace |
| Explode | `F.explode(...)` | One row per token |
| Normalise | `F.lower(col("word"))` | Case-insensitive counting |
| Count | `.groupBy("word").count()` | Aggregate |
| Sort | `.orderBy(F.desc("count"))` | Most frequent first |
