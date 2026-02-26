"""Tests for GlueProcessor and EMRProcessor using mocked boto3 clients."""

from unittest.mock import MagicMock, patch

import pytest

from src.config.schema import EMRConfig, GlueConfig
from src.processors.emr import EMRProcessor
from src.processors.glue import GlueProcessor


@pytest.fixture
def glue_config(sample_glue_config):
    return GlueConfig.model_validate(sample_glue_config)


@pytest.fixture
def emr_config(sample_emr_config):
    return EMRConfig.model_validate(sample_emr_config)


class TestGlueProcessor:
    def test_run_job(self, glue_config):
        with patch("src.processors.glue.boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client
            mock_client.start_job_run.return_value = {"JobRunId": "jr-123"}

            processor = GlueProcessor(glue_config)
            result = processor.run_job()

        mock_client.start_job_run.assert_called_once_with(
            JobName="test-glue-job",
            Arguments={"--enable-metrics": "true"},
        )
        assert result["job_run_id"] == "jr-123"
        assert result["status"] == "STARTING"

    def test_get_job_status(self, glue_config):
        with patch("src.processors.glue.boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client
            mock_client.get_job_run.return_value = {
                "JobRun": {"JobRunState": "SUCCEEDED"}
            }

            processor = GlueProcessor(glue_config)
            status = processor.get_job_status("jr-123")

        assert status == "SUCCEEDED"
        mock_client.get_job_run.assert_called_once_with(
            JobName="test-glue-job", RunId="jr-123"
        )

    def test_wait_for_completion_success(self, glue_config):
        with patch("src.processors.glue.boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client
            mock_client.get_job_run.return_value = {
                "JobRun": {"JobRunState": "SUCCEEDED"}
            }
            with patch("src.processors.glue.time.sleep"):
                processor = GlueProcessor(glue_config)
                final_status = processor.wait_for_completion("jr-123", poll_interval=0)

        assert final_status == "SUCCEEDED"

    def test_wait_for_completion_failure_raises(self, glue_config):
        with patch("src.processors.glue.boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client
            mock_client.get_job_run.return_value = {
                "JobRun": {"JobRunState": "FAILED"}
            }
            with patch("src.processors.glue.time.sleep"):
                processor = GlueProcessor(glue_config)
                with pytest.raises(RuntimeError, match="FAILED"):
                    processor.wait_for_completion("jr-123", poll_interval=0)


class TestEMRProcessor:
    def test_run_job(self, emr_config):
        with patch("src.processors.emr.boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client
            mock_client.add_job_flow_steps.return_value = {"StepIds": ["s-ABC123"]}

            processor = EMRProcessor(emr_config)
            result = processor.run_job()

        mock_client.add_job_flow_steps.assert_called_once()
        assert result["step_id"] == "s-ABC123"
        assert result["status"] == "PENDING"

    def test_get_step_status(self, emr_config):
        with patch("src.processors.emr.boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client
            mock_client.list_steps.return_value = {
                "Steps": [{"Status": {"State": "COMPLETED"}}]
            }

            processor = EMRProcessor(emr_config)
            status = processor.get_step_status("s-ABC123")

        assert status == "COMPLETED"
        mock_client.list_steps.assert_called_once_with(
            ClusterId="j-ABCDEFG123456", StepIds=["s-ABC123"]
        )

    def test_wait_for_completion_success(self, emr_config):
        with patch("src.processors.emr.boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client
            mock_client.list_steps.return_value = {
                "Steps": [{"Status": {"State": "COMPLETED"}}]
            }
            with patch("src.processors.emr.time.sleep"):
                processor = EMRProcessor(emr_config)
                final = processor.wait_for_completion("s-ABC123", poll_interval=0)

        assert final == "COMPLETED"

    def test_wait_for_completion_failure_raises(self, emr_config):
        with patch("src.processors.emr.boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client
            mock_client.list_steps.return_value = {
                "Steps": [{"Status": {"State": "FAILED"}}]
            }
            with patch("src.processors.emr.time.sleep"):
                processor = EMRProcessor(emr_config)
                with pytest.raises(RuntimeError, match="FAILED"):
                    processor.wait_for_completion("s-ABC123", poll_interval=0)
