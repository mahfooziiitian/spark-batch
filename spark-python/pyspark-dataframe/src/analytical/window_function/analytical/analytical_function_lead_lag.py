import os

from pyspark.sql import functions, SparkSession, Window
from pyspark.sql.functions import col

if __name__ == '__main__':
    spark = SparkSession.builder.appName('window').master("local[*]").getOrCreate()

    # Loading data into dataframe
    data_home = os.environ['DATA_HOME'].replace('\\', '/')

    data_path = f"{data_home}/FileData/Csv/employees.csv"

    df = spark.read.csv(data_path, inferSchema=True, header=True)

    df.printSchema()

    """
    SELECT name, salary,
        LAG(salary) OVER (PARTITION BY dept ORDER BY salary) AS lag,
        LEAD(salary, 1, 0) OVER (PARTITION BY dept ORDER BY salary) AS lead
    FROM employees;
    """

    windowSpec = (Window
                  .partitionBy("dept")
                  .orderBy(col("salary"))
                  )

    lag_salary = functions.lag("salary", 1, 0).over(windowSpec)
    lead_default_salary = functions.lead("salary", 1, 0).over(windowSpec)

    (df.select(
        df['name'],
        df['dept'],
        df['salary'],
        lag_salary.alias("lag"),
        lead_default_salary.alias("lead")
    ).show(truncate=False))
