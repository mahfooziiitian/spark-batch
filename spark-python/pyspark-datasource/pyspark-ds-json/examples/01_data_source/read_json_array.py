import json

from pyspark.sql.functions import col, explode, inline

from pys_json import get_spark

if __name__ == "__main__":
    spark = get_spark("json_array")

    json_string = json.dumps(
        {"key1": 0.75, "values": [{"id": 2313, "val1": 350, "val2": 6000}, {"id": 2477, "val1": 340, "val2": 6500}]}
    )
    df = spark.read.json(spark.sparkContext.parallelize([json_string]))

    df.printSchema()

    df.select("key1", "values.id", "values.val1", "values.val2").show()

    # using explode
    df.select("key1", explode(col("values")).alias("values")).select("key1", "values.*").show()

    # using inline function
    df.select("key1", inline(col("values"))).show()
