from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType

if __name__ == '__main__':
    spark = SparkSession.builder.appName('array-schema-fields').getOrCreate()
    schemaUntyped = (StructType()
                     .add("a", "integer")
                     .add("b", "string")
                     )
    print(schemaUntyped.simpleString())
    print(schemaUntyped.jsonValue())
    print(schemaUntyped.typeName())

    schemaUntyped = (StructType()
                     .add("a", IntegerType())
                     .add("b", StringType())
                     )

    print(schemaUntyped.simpleString())
    print(schemaUntyped.jsonValue())
    print(schemaUntyped.typeName())