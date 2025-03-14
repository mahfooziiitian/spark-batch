if __name__ == '__main__':
    spark.sql("""
        SELECT id, v,
            LEAD(v, 0) IGNORE NULLS OVER w lead,
            LAG(v, 0) IGNORE NULLS OVER w lag,
            NTH_VALUE(v, 2) IGNORE NULLS OVER w nth_value,
            FIRST_VALUE(v) IGNORE NULLS OVER w first_value,
            LAST_VALUE(v) IGNORE NULLS OVER w last_value
            FROM test_ignore_null
            WINDOW w AS (ORDER BY id)
            ORDER BY id;
    """)