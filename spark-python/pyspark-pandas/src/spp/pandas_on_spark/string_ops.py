"""Pandas API on Spark — string operations.

Demonstrates the ``.str`` accessor on pandas-on-Spark Series for common
string transformations: case, contains, split, replace, regex extract.

Usage::

    from spp.pandas_on_spark.string_ops import create_text_data
"""

import spp._env  # noqa: F401
import pyspark.pandas as ps


def create_text_data() -> ps.DataFrame:
    """Create a sample DataFrame with string columns."""
    return ps.DataFrame(
        {
            "full_name": [
                "Alice Smith",
                "Bob Jones",
                "Carol Williams",
                "Dave Brown",
                "Eve Davis",
            ],
            "email": [
                "alice@example.com",
                "BOB@WORK.ORG",
                "carol@example.com",
                "dave@work.org",
                "EVE@EXAMPLE.COM",
            ],
            "phone": [
                "123-456-7890",
                "234-567-8901",
                "345.678.9012",
                "(456) 789-0123",
                "567 890 1234",
            ],
        }
    )


def demo_case_ops(psdf: ps.DataFrame) -> None:
    """Case transformations."""
    print("=== upper / lower / title ===")
    psdf = psdf.copy()
    psdf["upper"] = psdf["full_name"].str.upper()
    psdf["lower"] = psdf["email"].str.lower()
    psdf["title"] = psdf["full_name"].str.title()
    print(psdf[["full_name", "upper", "lower", "title"]])


def demo_contains_startswith(psdf: ps.DataFrame) -> None:
    """Filtering with contains / startswith / endswith."""
    print("=== contains 'example' (case-insensitive) ===")
    print(psdf[psdf["email"].str.lower().str.contains("example")])

    print("\n=== startswith 'Alice' ===")
    print(psdf[psdf["full_name"].str.startswith("Alice")])


def demo_split_extract(psdf: ps.DataFrame) -> None:
    """Split and extract substrings.

    pyspark.pandas ``str.split(expand=True)`` has limitations, so we drop
    to the Spark DataFrame API for reliable column splitting.
    """
    from pyspark.sql import functions as F

    print("=== split full_name → first_name, last_name ===")
    sdf = psdf.to_spark(index_col="__index__")
    name_parts = F.split(F.col("full_name"), " ")
    sdf = sdf.withColumn("first_name", name_parts.getItem(0)).withColumn(
        "last_name", name_parts.getItem(1)
    )
    result = sdf.pandas_api(index_col="__index__")
    print(result[["full_name", "first_name", "last_name"]])

    print("\n=== extract email domain ===")
    sdf2 = psdf.to_spark(index_col="__index__")
    domain_parts = F.split(F.lower(F.col("email")), "@")
    sdf2 = sdf2.withColumn("domain", domain_parts.getItem(1))
    result2 = sdf2.pandas_api(index_col="__index__")
    print(result2[["email", "domain"]])


def demo_replace(psdf: ps.DataFrame) -> None:
    """String replacement and cleaning."""
    print("=== replace non-digit chars in phone ===")
    psdf = psdf.copy()
    psdf["phone_clean"] = psdf["phone"].str.replace(r"[^0-9]", "", regex=True)
    print(psdf[["phone", "phone_clean"]])


def demo_length_strip(psdf: ps.DataFrame) -> None:
    """String length and stripping."""
    print("=== string length ===")
    psdf = psdf.copy()
    psdf["name_len"] = psdf["full_name"].str.len()
    print(psdf[["full_name", "name_len"]])


def main() -> None:
    psdf = create_text_data()
    print("=== Text Data ===")
    print(psdf)
    print()
    demo_case_ops(psdf)
    print()
    demo_contains_startswith(psdf)
    print()
    demo_split_extract(psdf)
    print()
    demo_replace(psdf)
    print()
    demo_length_strip(psdf)


if __name__ == "__main__":
    from spp.session import create_spark_session

    spark = create_spark_session("ps-string-ops")
    main()
    spark.stop()
