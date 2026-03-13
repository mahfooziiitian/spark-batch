"""Array flattening XPath examples.

Demonstrates exploding xpath() arrays into individual rows, zipping
parallel arrays, and combining with other extracted fields using
CTEs and LATERAL VIEW.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("xml_xpath_flatten").getOrCreate()

    data = [
        "<catalog><store>Downtown</store>"
        "<item><name>Laptop</name><price>999.99</price><tag>electronics</tag><tag>computing</tag></item>"
        "<item><name>Mouse</name><price>29.99</price><tag>electronics</tag><tag>accessory</tag></item>"
        "<item><name>Desk</name><price>249.00</price><tag>furniture</tag><tag>office</tag></item>"
        "</catalog>",

        "<catalog><store>Airport</store>"
        "<item><name>Headphones</name><price>199.00</price><tag>electronics</tag><tag>audio</tag></item>"
        "<item><name>Charger</name><price>39.99</price><tag>electronics</tag><tag>accessory</tag></item>"
        "</catalog>",
    ]

    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("catalogs")

    # 1. Basic explode: flatten item names into rows
    print("=== Explode item names ===")
    spark.sql("""
        SELECT
            xpath_string(data, 'catalog/store')                AS store,
            explode(xpath(data, 'catalog/item/name/text()'))   AS item_name
        FROM catalogs
    """).show()

    # 2. Zip parallel arrays using arrays_zip + explode
    print("=== Zip names + prices ===")
    spark.sql("""
        SELECT
            xpath_string(data, 'catalog/store')  AS store,
            zipped.`0`                           AS item_name,
            zipped.`1`                           AS item_price
        FROM catalogs
        LATERAL VIEW explode(
            arrays_zip(
                xpath(data, 'catalog/item/name/text()'),
                xpath(data, 'catalog/item/price/text()')
            )
        ) t AS zipped
    """).show(truncate=False)

    # 3. Flatten tags with posexplode for position tracking
    print("=== All tags with position ===")
    spark.sql("""
        SELECT
            xpath_string(data, 'catalog/store')                AS store,
            pos + 1                                            AS tag_position,
            tag
        FROM catalogs
        LATERAL VIEW posexplode(xpath(data, 'catalog/item/tag/text()')) AS pos, tag
    """).show()

    # 4. CTE + explode: items per store with computed fields
    print("=== Items with computed total ===")
    spark.sql("""
        WITH store_items AS (
            SELECT
                xpath_string(data, 'catalog/store')  AS store,
                arrays_zip(
                    xpath(data, 'catalog/item/name/text()'),
                    xpath(data, 'catalog/item/price/text()')
                ) AS items
            FROM catalogs
        )
        SELECT
            store,
            item.`0`                                 AS name,
            CAST(item.`1` AS DOUBLE)                 AS price,
            ROUND(CAST(item.`1` AS DOUBLE) * 1.08, 2) AS price_with_tax
        FROM store_items
        LATERAL VIEW explode(items) t AS item
        ORDER BY store, price DESC
    """).show(truncate=False)

    # 5. Aggregation after flattening
    print("=== Store-level aggregation ===")
    spark.sql("""
        WITH flattened AS (
            SELECT
                xpath_string(data, 'catalog/store')                AS store,
                explode(xpath(data, 'catalog/item/price/text()'))  AS price_str
            FROM catalogs
        )
        SELECT
            store,
            COUNT(*)                              AS item_count,
            ROUND(SUM(CAST(price_str AS DOUBLE)), 2)  AS total_value,
            ROUND(AVG(CAST(price_str AS DOUBLE)), 2)  AS avg_price
        FROM flattened
        GROUP BY store
    """).show(truncate=False)
