# Date time

When dealing with XML data in PySpark—especially involving timestamp and date formats—you often need to parse or format the dates correctly when reading from or writing to XML.

## Reading XML with Date/Timestamp Handling

1. Use .schema() to control how Spark interprets date fields.
2. TimestampType expects format: "yyyy-MM-dd HH:mm:ss" or ISO format.
3. DateType expects format: "yyyy-MM-dd".

## 🔄 2. Custom Date/Timestamp Formats

If your XML has non-standard date formats (e.g. dd-MM-yyyy or MM/dd/yyyy HH:mm), Spark won't parse them automatically into DateType or TimestampType.

### Read as String first, then convert

```python
from pyspark.sql.functions import to_date, to_timestamp

df_raw = spark.read.format("xml") \
    .option("rowTag", "record") \
    .load("path/to/your.xml")

# Convert string fields into proper Date and Timestamp
df = df_raw.withColumn("birth_date", to_date("birth_date", "dd-MM-yyyy")) \
           .withColumn("created_at", to_timestamp("created_at", "MM/dd/yyyy HH:mm"))

df.show()
df.printSchema()
```
