"""
explode_outer() will return each and every individual value from an array.
If the array is empty or null, it returns null and go to the next array in an array type column in PySpark DataFrame.
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]

if __name__ == "__main__":
    # create an app named array_explode_with_no_array_value
    spark_app = SparkSession.builder.appName(
        "array_explode_with_no_array_value"
    ).getOrCreate()

    # consider an array with 5 elements
    my_array_data = [(1, []), (2, []), (3, []), (4, ["A", "B"]), (3, [])]

    # define the StructType and StructFields
    # for the above data
    schema = StructType(
        [
            StructField("Student_category", IntegerType()),
            StructField("Student_full_name", ArrayType(StringType())),
        ]
    )

    # create the dataframe and add schema to the dataframe
    df = spark_app.createDataFrame(my_array_data, schema=schema)

    # explode the Student_full_name column
    df.select("Student_full_name", explode_outer("Student_full_name")).show()
# end main
