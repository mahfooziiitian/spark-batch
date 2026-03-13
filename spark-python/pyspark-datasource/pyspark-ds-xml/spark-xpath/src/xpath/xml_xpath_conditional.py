"""Conditional XPath extraction examples.

Demonstrates WHERE filtering with xpath_boolean, multi-branch CASE
expressions, COALESCE fallbacks, and IF() for inline conditions
on XML employee records.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("xml_xpath_conditional").getOrCreate()

    data = [
        "<employee><id>E001</id><name>Alice</name><department>Engineering</department>"
        "<salary>120000</salary><level>Senior</level><remote>true</remote>"
        "<bonus_pct>0.15</bonus_pct></employee>",

        "<employee><id>E002</id><name>Bob</name><department>Sales</department>"
        "<salary>85000</salary><level>Mid</level><remote>false</remote>"
        "<bonus_pct>0.10</bonus_pct></employee>",

        "<employee><id>E003</id><name>Carol</name><department>Engineering</department>"
        "<salary>95000</salary><level>Mid</level><remote>true</remote>"
        "<bonus_pct>0.12</bonus_pct></employee>",

        "<employee><id>E004</id><name>Dave</name><department>HR</department>"
        "<salary>70000</salary><level>Junior</level><remote>false</remote>"
        "<bonus_pct>0.05</bonus_pct></employee>",

        "<employee><id>E005</id><name>Eve</name><department>Engineering</department>"
        "<salary>150000</salary><level>Lead</level><remote>true</remote>"
        "<bonus_pct>0.20</bonus_pct></employee>",
    ]

    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("employees")

    # 1. Filter with xpath_boolean in WHERE clause
    print("=== High earners (salary >= 100000) ===")
    spark.sql("""
        SELECT
            xpath_string(data, 'employee/name')       AS name,
            xpath_int(data, 'employee/salary')         AS salary,
            xpath_string(data, 'employee/department')  AS dept
        FROM employees
        WHERE xpath_boolean(data, 'employee[salary >= 100000]')
    """).show()

    # 2. Multi-branch CASE on extracted values
    print("=== Salary bands ===")
    spark.sql("""
        SELECT
            xpath_string(data, 'employee/name')       AS name,
            xpath_int(data, 'employee/salary')         AS salary,
            CASE
                WHEN xpath_int(data, 'employee/salary') >= 140000 THEN 'Band 4 - Executive'
                WHEN xpath_int(data, 'employee/salary') >= 100000 THEN 'Band 3 - Senior'
                WHEN xpath_int(data, 'employee/salary') >= 80000  THEN 'Band 2 - Mid'
                ELSE 'Band 1 - Entry'
            END AS salary_band
        FROM employees
    """).show(truncate=False)

    # 3. COALESCE fallback for missing/empty elements
    print("=== COALESCE for defaults ===")
    spark.sql("""
        SELECT
            xpath_string(data, 'employee/name')  AS name,
            COALESCE(
                NULLIF(xpath_string(data, 'employee/title'), ''),
                xpath_string(data, 'employee/level')
            ) AS display_title
        FROM employees
    """).show()

    # 4. Combined boolean + CASE: compute bonus
    print("=== Bonus calculation ===")
    spark.sql("""
        SELECT
            xpath_string(data, 'employee/name')                        AS name,
            xpath_int(data, 'employee/salary')                         AS salary,
            xpath_string(data, 'employee/remote')                      AS remote,
            xpath_double(data, 'employee/bonus_pct')                   AS bonus_pct,
            CASE
                WHEN xpath_boolean(data, 'employee[remote="true"]')
                    THEN ROUND(xpath_int(data, 'employee/salary')
                         * xpath_double(data, 'employee/bonus_pct') * 1.1, 2)
                ELSE ROUND(xpath_int(data, 'employee/salary')
                     * xpath_double(data, 'employee/bonus_pct'), 2)
            END AS bonus_amount
        FROM employees
    """).show(truncate=False)
