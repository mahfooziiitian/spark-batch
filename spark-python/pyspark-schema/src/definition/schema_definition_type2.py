from pyspark.sql.types import StructType, StructField, MapType, LongType, StringType

if __name__ == '__main__':
    schemaWithMap = StructType([
        StructField("map", MapType(LongType(), StringType()), False)
    ])

    print(schemaWithMap.simpleString())
    print(schemaWithMap.jsonValue())
    print(schemaWithMap.typeName())

