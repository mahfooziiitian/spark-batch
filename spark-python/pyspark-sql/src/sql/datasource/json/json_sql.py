import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    derby_home = os.environ["derby.system.home"]
    spark = (SparkSession
             .builder
             .master("local[*]")
             .appName("json-sql")
             .config("spark.sql.warehouse.dir", os.environ["SPARK_WAREHOUSE"])
             .config("spark.driver.extraJavaOptions",
                     f"-Dderby.system.home='{derby_home}'")
             .enableHiveSupport()
             .getOrCreate()
             )

    spark.sql("drop table if exists store_data")
    spark.sql("""
    CREATE TABLE store_data AS SELECT
        '{
           "store":{
              "fruit": [
                {"weight":8,"type":"apple"},
                {"weight":9,"type":"pear"}
              ],
              "basket":[
                [1,2,{"b":"y","a":"x"}],
                [3,4],
                [5,6]
              ],
              "book":[
                {
                  "author":"Nigel Rees",
                  "title":"Sayings of the Century",
                  "category":"reference",
                  "price":8.95
                },
                {
                  "author":"Herman Melville",
                  "title":"Moby Dick",
                  "category":"fiction",
                  "price":8.99,
                  "isbn":"0-553-21311-3"
                },
                {
                  "author":"J. R. R. Tolkien",
                  "title":"The Lord of the Rings",
                  "category":"fiction",
                  "reader":[
                    {"age":25,"name":"bob"},
                    {"age":26,"name":"jack"}
                  ],
                  "price":22.99,
                  "isbn":"0-395-19395-8"
                }
              ],
              "bicycle":{
                "price":19.95,
                "color":"red"
              }
            },
            "owner":"amy",
            "zip code":"94025",
            "fb:testid":"1234"
         }' as raw
    """)

    spark.sql("SELECT * FROM store_data").show(truncate=False)
