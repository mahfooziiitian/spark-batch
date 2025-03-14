import pandas as pd
import pyarrow as pa
from pyspark import Row


def test_pyspark_conversion(spark):
    # Create a Pandas DataFrame
    pdf = pd.DataFrame({
        'id': [1, 2, 3],
        'value': ['a', 'b', 'c']
    })

    # Convert Pandas DataFrame to PyArrow Table
    table = pa.Table.from_pandas(pdf)

    # Convert PyArrow Table to PySpark DataFrame
    sdf = spark.createDataFrame(table.to_pandas())

    # Check the schema and content of the DataFrame
    assert sdf.schema.fieldNames() == ['id', 'value']
    data = sdf.collect()
    assert data == [Row(id=1, value='a'), Row(id=2, value='b'), Row(id=3, value='c')]