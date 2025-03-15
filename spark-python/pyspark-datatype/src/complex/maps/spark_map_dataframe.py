from pyspark.sql import SparkSession
from pyspark.sql.functions import col, map_concat, map_keys, map_values
from pyspark.sql.types import ArrayType, MapType, Row, StringType, StructType

if __name__ == "__main__":
    """Spark MapType class extends DataType class which is a superclass of all types in Spark and it takes two
    mandatory arguments "keyType" and "valueType" of type DataType and one optional boolean argument
    valueContainsNull.
    keyType and valueType can be any type that extends the DataType class.
    Keys in a maps are not allowed to have `null` values.
    """
    spark = (
        SparkSession.builder.master("local[*]").appName("spark_map_df").getOrCreate()
    )

    # Creating MapType maps column on Spark DataFrame
    arrayStructureData = [
        Row(
            "James",
            [Row("Newark", "NY"), Row("Brooklyn", "NY")],
            {"hair": "black", "eye": "brown"},
            {"height": "5.9"},
        ),
        Row(
            "Michael",
            [Row("SanJose", "CA"), Row("Sandiago", "CA")],
            {"hair": "brown", "eye": "black"},
            {"height": "6"},
        ),
        Row(
            "Robert",
            [Row("LasVegas", "NV")],
            {"hair": "red", "eye": "gray"},
            {"height": "6.3"},
        ),
        Row("Maria", None, {"hair": "blond", "eye": "red"}, {"height": "5.6"}),
        Row(
            "Jen",
            [Row("LAX", "CA"), Row("Orange", "CA")],
            {"white": "black", "eye": "black"},
            {"height": "5.2"},
        ),
    ]

    mapType = MapType(StringType(), StringType())

    arrayStructureSchema = (
        StructType()
        .add("name", StringType())
        .add(
            "addresses",
            ArrayType(
                StructType().add("city", StringType()).add("state", StringType())
            ),
        )
        .add("properties", mapType)
        .add("secondProp", MapType(StringType(), StringType()))
    )

    mapTypeDF = spark.createDataFrame(
        spark.sparkContext.parallelize(arrayStructureData), arrayStructureSchema
    )
    mapTypeDF.printSchema()
    mapTypeDF.show()

    #  Getting all maps Keys from DataFrame MapType column
    mapTypeDF.select(col("name"), map_keys(col("properties"))).show(truncate=False)

    # Getting all maps values from the DataFrame MapType column
    mapTypeDF.select(col("name"), map_values(col("properties"))).show(truncate=False)

    # Merging maps using map_concat()

    mapTypeDF.select(
        col("name"), map_concat(col("properties"), col("secondProp"))
    ).show(truncate=False)
