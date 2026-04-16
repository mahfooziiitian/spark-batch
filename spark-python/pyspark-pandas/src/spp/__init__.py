"""PySpark Pandas — pandas interop, pandas UDFs, pandas-on-Spark, and UDTFs.

Reusable modules
----------------
- ``spp.session``          — SparkSession factory
- ``spp.dataframe``        — pandas ↔ Spark DataFrame interop
- ``spp.arrow_optimization`` — Arrow benchmark utilities
- ``spp.pandas_udf``       — Pandas UDFs (series, aggregate, grouped map, cogroup)
- ``spp.pandas_on_spark``  — Pandas API on Spark (DataFrame, groupby, I/O, missing data)
- ``spp.udtf``             — User-Defined Table Functions
- ``spp.integration``      — Integration patterns & real-world workflows
"""

import os

import spp._env  # noqa: F401  (early env setup)

from spp.session import create_spark_session

__all__ = ["create_spark_session"]
