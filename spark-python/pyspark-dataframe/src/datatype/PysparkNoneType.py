from pyspark.sql import SparkSession
from pyspark.sql.functions import when, expr

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .appName("spark dataframe")
             .master("local[*]")
             .getOrCreate())

    data = [("James", "M", 60000), ("Michael", "M", 70000),
            ("Robert", None, 400000), ("Maria", "F", 500000),
            ("Jen", "", None)]

    columns = ["name", "gender", "salary"]
    df = spark.createDataFrame(data=data, schema=columns)
    df.show(truncate=False)

    # Using when() otherwise()

    df2 = df.withColumn("new_gender", when(df.gender == "M", "Male")
                        .when(df.gender == "F", "Female")
                        .when(df.gender.isNull(), "")
                        .otherwise(df.gender))
    df2.show()

    # Using Case When on withColumn()
    df3 = df.withColumn("new_gender", expr("CASE WHEN gender = 'M' THEN 'Male' " +
                                           "WHEN gender = 'F' THEN 'Female' WHEN gender IS NULL THEN ''" +
                                           "ELSE gender END"))
    df3.show(truncate=False)
