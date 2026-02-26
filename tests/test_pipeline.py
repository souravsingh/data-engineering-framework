"""Tests for the Pipeline orchestrator."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.config.loader import ConfigLoader
from src.pipeline import Pipeline


@pytest.fixture
def pipeline_config(sample_pipeline_config):
    return ConfigLoader().load_from_dict(sample_pipeline_config)


class TestPipeline:
    def test_pipeline_init(self, pipeline_config):
        """Pipeline should instantiate all sub-components without error."""
        with patch("src.pipeline.GlueProcessor"):
            pipeline = Pipeline(pipeline_config)

        assert pipeline.config is pipeline_config
        assert pipeline.connector is not None
        assert pipeline.bronze is not None
        assert pipeline.silver is not None
        assert pipeline.gold is not None
        assert pipeline.writer is not None

    def test_pipeline_selects_emr_processor(self, sample_pipeline_config):
        sample_pipeline_config["transformation"] = {
            "engine": "emr",
            "emr": {
                "cluster_id": "j-XYZ",
                "region": "us-east-1",
                "spark_script": "s3://bucket/script.py",
            },
        }
        config = ConfigLoader().load_from_dict(sample_pipeline_config)
        with patch("src.pipeline.EMRProcessor") as mock_emr:
            Pipeline(config)
            mock_emr.assert_called_once()

    def test_pipeline_run_mocked(self, pipeline_config):
        """run() should call each layer and return a success summary."""
        fake_df = pd.DataFrame({"id": [1, 2], "val": [10, 20]})
        fake_data = {"orders": fake_df}

        with patch("src.pipeline.GlueProcessor"):
            pipeline = Pipeline(pipeline_config)

        pipeline.connector = MagicMock()
        pipeline.connector.read_all_tables.return_value = fake_data

        pipeline.bronze = MagicMock()
        pipeline.bronze.ingest.return_value = {"orders": "/tmp/bronze/orders/"}  # noqa: S108

        pipeline.silver = MagicMock()
        pipeline.silver.transform.return_value = fake_data

        pipeline.gold = MagicMock()
        pipeline.gold.aggregate.return_value = fake_data

        result = pipeline.run()

        pipeline.connector.read_all_tables.assert_called_once()
        pipeline.bronze.ingest.assert_called_once_with(fake_data, pipeline.writer)
        pipeline.silver.transform.assert_called_once_with(fake_data, pipeline.writer)
        pipeline.gold.aggregate.assert_called_once_with(fake_data, pipeline.writer)

        assert result["status"] == "success"
        assert result["pipeline"] == "test_pipeline"
        assert "bronze_paths" in result
        assert "silver_tables" in result
        assert "gold_tables" in result
