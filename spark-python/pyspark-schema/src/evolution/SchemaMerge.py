if __name__ == '__main__':
    loans = sql("""
                SELECT addr_state, CAST(rand(10)*count as bigint) AS count,
                CAST(rand(10) * 10000 * count AS double) AS amount
                FROM loan_by_state_delta
                """)

    # Show original DataFrame's schema
    loans.printSchema()
    DELTALAKE_PATH = ""
    loans.write.format("delta") \
        .mode("append") \
        .save(DELTALAKE_PATH)

    DELTALAKE_SILVER_PATH = ""

    loans.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("append") \
        .save(DELTALAKE_SILVER_PATH)
