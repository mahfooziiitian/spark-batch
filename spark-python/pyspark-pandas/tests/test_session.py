"""Tests for the reusable SparkSession factory."""

import pytest
from pyspark.sql import SparkSession

from spp.session import create_spark_session


class TestSparkSession:
    def test_session_is_spark_session(self, spark):
        assert isinstance(spark, SparkSession)

    def test_session_is_running(self, spark):
        assert spark.sparkContext._jsc is not None

    def test_arrow_enabled(self, spark):
        assert spark.conf.get("spark.sql.execution.arrow.pyspark.enabled") == "true"

    def test_aqe_enabled(self, spark):
        assert spark.conf.get("spark.sql.adaptive.enabled") == "true"

    def test_shuffle_partitions(self, spark):
        assert spark.conf.get("spark.sql.shuffle.partitions") == "2"

    def test_create_with_custom_name(self):
        session = create_spark_session(
            "test-custom",
            shuffle_partitions=1,
            log_level="ERROR",
            extra_configs={"spark.ui.enabled": "false"},
        )
        assert isinstance(session, SparkSession)
        session.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
