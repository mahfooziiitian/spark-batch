"""Numeric XPath extraction examples.

Demonstrates xpath_int, xpath_double, xpath_number, and arithmetic
operations on values extracted from XML order data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("xml_xpath_numeric").getOrCreate()

    data = [
        "<order><id>1001</id><product>Widget A</product>"
        "<quantity>5</quantity><unit_price>29.99</unit_price>"
        "<discount>0.10</discount><tax_rate>0.08</tax_rate></order>",

        "<order><id>1002</id><product>Widget B</product>"
        "<quantity>12</quantity><unit_price>14.50</unit_price>"
        "<discount>0.0</discount><tax_rate>0.08</tax_rate></order>",

        "<order><id>1003</id><product>Widget C</product>"
        "<quantity>1</quantity><unit_price>199.00</unit_price>"
        "<discount>0.15</discount><tax_rate>0.10</tax_rate></order>",

        "<order><id>1004</id><product>Widget A</product>"
        "<quantity>100</quantity><unit_price>29.99</unit_price>"
        "<discount>0.20</discount><tax_rate>0.08</tax_rate></order>",
    ]

    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("orders")

    # Extract numeric fields and compute totals
    spark.sql("""
        SELECT
            xpath_int(data, 'order/id')                              AS order_id,
            xpath_string(data, 'order/product')                      AS product,
            xpath_int(data, 'order/quantity')                        AS qty,
            xpath_double(data, 'order/unit_price')                   AS price,
            xpath_double(data, 'order/discount')                     AS discount,
            xpath_double(data, 'order/tax_rate')                     AS tax_rate,
            ROUND(
                xpath_int(data, 'order/quantity')
                * xpath_double(data, 'order/unit_price')
                * (1 - xpath_double(data, 'order/discount')),
                2
            )                                                        AS subtotal,
            ROUND(
                xpath_int(data, 'order/quantity')
                * xpath_double(data, 'order/unit_price')
                * (1 - xpath_double(data, 'order/discount'))
                * (1 + xpath_double(data, 'order/tax_rate')),
                2
            )                                                        AS total_with_tax
        FROM orders
    """).show(truncate=False)

    # Aggregation on extracted values
    spark.sql("""
        SELECT
            xpath_string(data, 'order/product')                      AS product,
            SUM(xpath_int(data, 'order/quantity'))                    AS total_qty,
            ROUND(SUM(
                xpath_int(data, 'order/quantity')
                * xpath_double(data, 'order/unit_price')
            ), 2)                                                    AS gross_revenue
        FROM orders
        GROUP BY xpath_string(data, 'order/product')
    """).show(truncate=False)
