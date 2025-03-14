import pandas as pd
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("CreateDataFrame").getOrCreate()

    d = {
        "key1": 0.75,
        "values": [
            {
                "id": 2313,
                "val1": 350,
                "val2": 6000
            },
            {
                "id": 2477,
                "val1": 340,
                "val2": 6500
            }
        ]
    }
    # We need to put this data into columnar format for pandas
    df_dict = {
        'key1': [d['key1'] for _ in range(len(d['values']))],
        'id': [x['id'] for x in d['values']],
        'val1': [x['val1'] for x in d['values']],
        'val2': [x['val2'] for x in d['values']],
    }

    pdf = pd.DataFrame.from_dict(df_dict)

    df = spark.createDataFrame(pdf)
    df.show()
