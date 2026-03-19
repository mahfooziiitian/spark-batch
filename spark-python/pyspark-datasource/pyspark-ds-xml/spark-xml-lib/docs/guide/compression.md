# Compression

Read and write compressed XML files using standard codecs.

---

## Supported Codecs

| Codec | Option Value | File Extension | Notes |
|---|---|---|---|
| gzip | `"gzip"` | `.xml.gz` | Best compatibility |
| bzip2 | `"bzip2"` | `.xml.bz2` | Better compression ratio |
| deflate | `"deflate"` | `.xml.deflate` | Raw deflate |
| snappy | `"snappy"` | `.xml.snappy` | Fastest read/write |

---

## Write Compressed XML

```python
data = [("John", 28), ("Anna", 23), ("Peter", 34)]
df = spark.createDataFrame(data, ["Name", "Age"])

(
    df.write.format("xml")
    .mode("overwrite")
    .option("rootTag", "people")
    .option("rowTag", "person")
    .option("compression", "gzip")
    .save(xml_file)
)
```

---

## Read Compressed XML

```python
df = (
    spark.read.format("xml")
    .option("rowTag", "person")
    .option("compression", "gzip")
    .load(xml_file)
)
df.show()
```

---

## Complete Round-Trip

=== "gzip"

    ```python
    df.write.format("xml").mode("overwrite") \
        .option("rootTag", "people").option("rowTag", "person") \
        .option("compression", "gzip").save(path)

    df = spark.read.format("xml") \
        .option("rowTag", "person").option("compression", "gzip").load(path)
    ```

=== "bzip2"

    ```python
    df.write.format("xml").mode("overwrite") \
        .option("rootTag", "people").option("rowTag", "person") \
        .option("compression", "bzip2").save(path)

    df = spark.read.format("xml") \
        .option("rowTag", "person").option("compression", "bzip2").load(path)
    ```

=== "snappy"

    ```python
    df.write.format("xml").mode("overwrite") \
        .option("rootTag", "people").option("rowTag", "person") \
        .option("compression", "snappy").save(path)

    df = spark.read.format("xml") \
        .option("rowTag", "person").option("compression", "snappy").load(path)
    ```

=== "deflate"

    ```python
    df.write.format("xml").mode("overwrite") \
        .option("rootTag", "people").option("rowTag", "person") \
        .option("compression", "deflate").save(path)

    df = spark.read.format("xml") \
        .option("rowTag", "person").option("compression", "deflate").load(path)
    ```

!!! tip "Choosing a Codec"
    - **snappy** — fastest read/write, moderate compression
    - **gzip** — best compatibility, good compression
    - **bzip2** — best compression ratio, slowest
    - **deflate** — similar to gzip without headers

> **Source:** `src/spark_xml/compression/`
