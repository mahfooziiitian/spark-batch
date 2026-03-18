from pyspark.sql.functions import col

# normalize column names
df = df.toDF(*[c.strip().lower().replace(" ", "_") for c in df.columns])

# cast types explicitly (don’t rely only on inferSchema)
df = (df
      .withColumn("age", col("age").cast("int"))
      .withColumn("salary", col("salary").cast("double")))