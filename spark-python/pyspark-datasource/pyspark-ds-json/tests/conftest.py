"""Shared pytest configuration for all test modules."""

import os


def pytest_configure(config):
    """Set JAVA_HOME to Java 17 before any test collection."""
    _ = config  # required by pytest hook signature
    java_home_17 = os.environ.get("JAVA_HOME_17")
    if java_home_17:
        os.environ["JAVA_HOME"] = java_home_17
