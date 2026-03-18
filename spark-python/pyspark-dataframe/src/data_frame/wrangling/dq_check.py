from pyspark.sql.functions import sum

# null check
df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

# filter bad data
df = df.filter(col("salary") > 0)