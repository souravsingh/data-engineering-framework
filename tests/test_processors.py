"""Tests for GlueProcessor and EMRProcessor using mocked boto3 clients."""

from unittest.mock import MagicMock, patch

import pytest

from src.config.schema import EMRConfig, GlueConfig, RedshiftConfig
from src.processors.emr import EMRProcessor
from src.processors.glue import GlueProcessor


@pytest.fixture
def glue_config(sample_glue_config):
    return GlueConfig.model_validate(sample_glue_config)


@pytest.fixture
def glue_config_with_connection(sample_glue_config_with_connection):
    return GlueConfig.model_validate(sample_glue_config_with_connection)


@pytest.fixture
def emr_config(sample_emr_config):
    return EMRConfig.model_validate(sample_emr_config)


@pytest.fixture
def redshift_config(sample_redshift_config):
    return RedshiftConfig.model_validate(sample_redshift_config)


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

    def test_run_job_injects_redshift_connection_name(self, glue_config_with_connection):
        """When redshift_connection_name is set, it must appear in job arguments."""
        with patch("src.processors.glue.boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client
            mock_client.start_job_run.return_value = {"JobRunId": "jr-456"}

            processor = GlueProcessor(glue_config_with_connection)
            result = processor.run_job()

        call_kwargs = mock_client.start_job_run.call_args.kwargs
        assert call_kwargs["Arguments"]["--redshift_connection_name"] == "my-redshift-connection"
        assert result["job_run_id"] == "jr-456"

    def test_run_job_without_connection_name_no_extra_arg(self, glue_config):
        """When redshift_connection_name is absent, the argument must not be injected."""
        with patch("src.processors.glue.boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client
            mock_client.start_job_run.return_value = {"JobRunId": "jr-789"}

            processor = GlueProcessor(glue_config)
            processor.run_job()

        call_kwargs = mock_client.start_job_run.call_args.kwargs
        assert "--redshift_connection_name" not in call_kwargs["Arguments"]

    def test_create_redshift_connection(self, glue_config_with_connection, redshift_config):
        """create_redshift_connection should call create_connection with correct input."""
        with patch("src.processors.glue.boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client

            processor = GlueProcessor(glue_config_with_connection)
            name = processor.create_redshift_connection(redshift_config)

        assert name == "my-redshift-connection"
        mock_client.create_connection.assert_called_once()
        conn_input = mock_client.create_connection.call_args.kwargs["ConnectionInput"]
        assert conn_input["Name"] == "my-redshift-connection"
        assert conn_input["ConnectionType"] == "JDBC"
        assert "jdbc:redshift://" in conn_input["ConnectionProperties"]["JDBC_CONNECTION_URL"]
        assert conn_input["ConnectionProperties"]["USERNAME"] == redshift_config.username
        # Physical connection requirements should be populated
        phys = conn_input["PhysicalConnectionRequirements"]
        assert phys["SubnetId"] == "subnet-0abc1234"
        assert phys["SecurityGroupIdList"] == ["sg-0abc1234"]
        assert phys["AvailabilityZone"] == "us-east-1a"

    def test_create_redshift_connection_no_name_raises(self, glue_config, redshift_config):
        """Calling create_redshift_connection without a connection name must raise ValueError."""
        with patch("src.processors.glue.boto3.client"):
            processor = GlueProcessor(glue_config)
            with pytest.raises(ValueError, match="redshift_connection_name"):
                processor.create_redshift_connection(redshift_config)

    def test_get_redshift_connection(self, glue_config_with_connection):
        """get_redshift_connection should return the Connection dict from the API response."""
        expected = {"Name": "my-redshift-connection", "ConnectionType": "JDBC"}
        with patch("src.processors.glue.boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client
            mock_client.get_connection.return_value = {"Connection": expected}

            processor = GlueProcessor(glue_config_with_connection)
            result = processor.get_redshift_connection("my-redshift-connection")

        assert result == expected
        mock_client.get_connection.assert_called_once_with(Name="my-redshift-connection")


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
