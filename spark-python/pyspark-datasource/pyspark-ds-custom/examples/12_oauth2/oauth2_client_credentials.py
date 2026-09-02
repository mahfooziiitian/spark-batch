"""OAuth2 authentication — client credentials flow with REST API data source.

Demonstrates reading from an OAuth2-protected REST API using the
client_credentials grant type. The data source automatically fetches
an access token from the token endpoint before each request.

Prerequisites:
    Start the mock server: uv run python examples/mock_server/server.py

Run:
    uv run python examples/12_oauth2/oauth2_client_credentials.py
    uv run python examples/12_oauth2/oauth2_client_credentials.py --url http://myserver:9090

Supported OAuth2 flows:
    - client_credentials (machine-to-machine, most common for APIs)
    - password (resource owner, username/password)
    - bearer token (pre-obtained, no refresh)
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import functions as F

from custom_ds import RestApiDataSource, RestApiSinkDataSource, create_spark_session

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OAuth2 client credentials flow demo")
    parser.add_argument(
        "--url",
        default=os.environ.get("MOCK_SERVER_URL", "http://localhost:9090"),
        help="Mock server base URL (default: $MOCK_SERVER_URL or http://localhost:9090)",
    )
    args = parser.parse_args()
    base_url: str = args.url

    spark = create_spark_session("oauth2-demo")

    spark.dataSource.register(RestApiDataSource)
    spark.dataSource.register(RestApiSinkDataSource)

    # --- 1. Client Credentials Flow -------------------------------------------
    print("=" * 60)
    print("OAuth2 Client Credentials -- Read from protected endpoint")
    print("=" * 60)

    df = (
        spark.read.format("restapi")
        .option("url", f"{base_url}/api/protected/users")
        .option("resultKey", "data")
        .option("schema", "id LONG, name STRING, email STRING, city STRING, age LONG")
        .option("auth", "oauth2")
        .option("oauth.tokenUrl", f"{base_url}/oauth/token")
        .option("oauth.clientId", "test-client")
        .option("oauth.clientSecret", "test-secret")
        .option("oauth.scope", "read")
        .load()
    )

    df.show(truncate=False)
    print(f"Rows fetched: {df.count()}")

    # --- 2. Pre-obtained Token ------------------------------------------------
    print("=" * 60)
    print("OAuth2 Token -- Skip token endpoint")
    print("=" * 60)

    df_bearer = (
        spark.read.format("restapi")
        .option("url", f"{base_url}/api/protected/users")
        .option("resultKey", "data")
        .option("schema", "id LONG, name STRING, email STRING, city STRING, age LONG")
        .option("auth", "oauth2")
        .option("oauth.bearerToken", "mock-access-token-12345")
        .load()
    )

    df_bearer.show(5, truncate=False)

    # --- 3. Write with OAuth2 -------------------------------------------------
    print("=" * 60)
    print("OAuth2 -- Write to REST API with authentication")
    print("=" * 60)

    df_write = spark.range(5).select(
        F.col("id"),
        F.concat(F.lit("oauth-item-"), F.col("id").cast("string")).alias("value"),
    )

    df_write.write.format("restapi_sink").option("url", f"{base_url}/api/records").option(
        "auth", "oauth2"
    ).option("oauth.tokenUrl", f"{base_url}/oauth/token").option(
        "oauth.clientId", "test-client"
    ).option("oauth.clientSecret", "test-secret").mode("append").save()

    print("Write complete!")

    # --- 4. SQL with OAuth2 ---------------------------------------------------
    print("=" * 60)
    print("SQL analytics over OAuth2-protected data")
    print("=" * 60)

    df.createOrReplaceTempView("protected_users")

    spark.sql("""
        SELECT city, COUNT(*) as user_count, ROUND(AVG(age), 1) as avg_age
        FROM protected_users
        GROUP BY city
        ORDER BY user_count DESC
    """).show()

    spark.stop()
