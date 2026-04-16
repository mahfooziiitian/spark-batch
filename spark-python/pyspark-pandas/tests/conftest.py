"""Shared pytest fixtures for pyspark-pandas tests."""

import os

import pytest

# Resolve JAVA_HOME from JAVA_HOME_11 if not already set
if not os.environ.get("JAVA_HOME"):
    java_home_11 = os.environ.get("JAVA_HOME_11", "")
    if java_home_11:
        os.environ["JAVA_HOME"] = java_home_11

from spp.session import create_spark_session


@pytest.fixture(scope="session")
def spark():
    session = create_spark_session(
        "test-suite",
        shuffle_partitions=2,
        log_level="ERROR",
        extra_configs={"spark.ui.enabled": "false"},
    )
    yield session
    session.stop()
