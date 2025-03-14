from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, schema_of_json, lit

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .appName('infer_schema_using_schema_of_json')
             .getOrCreate()
             )

    # Sample data
    data = [(1, '{"name": "David", "age": 40}'),
            (2, '{"name": "Mahfooz", "age": 55}'),
            (3, '{"name": "Alam", "age": "66"}'),
            ]

    # Infer schema from JSON string
    schema = schema_of_json(lit('{"name": "Mahfooz", "age": 34}'))
    print(schema)

    # Create DataFrame
    df = spark.createDataFrame(data, ["id", "value"])

    # Parse JSON string using inferred schema
    df_parsed = df.withColumn("value", from_json(df.value, schema))
    df_parsed.show()
