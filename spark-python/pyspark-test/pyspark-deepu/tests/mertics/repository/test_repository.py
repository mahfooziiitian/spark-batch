"""Tests for PyDeequ FileSystemMetricsRepository."""

import pytest

from dpu.mertics.repository.repository import load_metrics, save_metrics
from dpu.sample_data import create_sample_df

try:
    from pydeequ.repository import FileSystemMetricsRepository
except ImportError:
    FileSystemMetricsRepository = None


class TestRepository:
    """Tests for FileSystemMetricsRepository save/load cycle."""

    def test_save_and_load_metrics(self, spark, tmp_path):
        metrics_file = FileSystemMetricsRepository.helper_metrics_file(spark, str(tmp_path / "metrics.json"))
        repository = FileSystemMetricsRepository(spark, metrics_file)
        df = create_sample_df(spark)

        save_metrics(spark, df, repository)
        result_df = load_metrics(repository)

        assert result_df.count() >= 1

    def test_loaded_metrics_contain_approx_count(self, spark, tmp_path):
        metrics_file = FileSystemMetricsRepository.helper_metrics_file(spark, str(tmp_path / "metrics.json"))
        repository = FileSystemMetricsRepository(spark, metrics_file)
        df = create_sample_df(spark)

        save_metrics(spark, df, repository)
        result_df = load_metrics(repository)

        names = {row["name"] for row in result_df.collect()}
        assert "ApproxCountDistinct" in names


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
