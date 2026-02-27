"""Shared pytest fixtures for the data engineering framework test suite."""

import pandas as pd
import pytest


@pytest.fixture
def sample_redshift_config():
    return {
        "host": "redshift.example.com",
        "port": 5439,
        "database": "testdb",
        "schema_name": "public",
        "username": "admin",
        "password": "secret",
        "tables": [
            {"name": "orders", "query": "SELECT * FROM orders", "schema_name": "public"},
        ],
    }


@pytest.fixture
def sample_glue_config():
    return {
        "job_name": "test-glue-job",
        "region": "us-east-1",
        "iam_role": "arn:aws:iam::123456789012:role/GlueRole",
        "script_location": "s3://bucket/scripts/job.py",
        "temp_dir": "s3://bucket/temp/",
        "arguments": {"--enable-metrics": "true"},
    }


@pytest.fixture
def sample_glue_config_with_connection():
    """GlueConfig dict that includes a named Glue Redshift connection."""
    return {
        "job_name": "test-glue-job-conn",
        "region": "us-east-1",
        "iam_role": "arn:aws:iam::123456789012:role/GlueRole",
        "script_location": "s3://bucket/scripts/job.py",
        "temp_dir": "s3://bucket/temp/",
        "arguments": {"--enable-metrics": "true"},
        "redshift_connection_name": "my-redshift-connection",
        "subnet_id": "subnet-0abc1234",
        "security_group_ids": ["sg-0abc1234"],
        "availability_zone": "us-east-1a",
    }


@pytest.fixture
def sample_emr_config():
    return {
        "cluster_id": "j-ABCDEFG123456",
        "region": "us-east-1",
        "spark_script": "s3://bucket/scripts/spark_job.py",
        "arguments": ["--input", "s3://bucket/input"],
    }


@pytest.fixture
def sample_medallion_config():
    return {
        "bronze": {"path": "/tmp/bronze", "format": "parquet"},  # noqa: S108
        "silver": {"path": "/tmp/silver", "format": "parquet"},  # noqa: S108
        "gold": {"path": "/tmp/gold", "format": "parquet"},  # noqa: S108
    }


@pytest.fixture
def sample_storage_config():
    return {"format": "parquet", "path": "/tmp/output"}  # noqa: S108


@pytest.fixture
def sample_pipeline_config(
    sample_redshift_config,
    sample_glue_config,
    sample_medallion_config,
    sample_storage_config,
):
    return {
        "name": "test_pipeline",
        "version": "1.0",
        "source": sample_redshift_config,
        "transformation": {
            "engine": "glue",
            "glue": sample_glue_config,
        },
        "medallion": sample_medallion_config,
        "storage": sample_storage_config,
    }


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "Name": ["  Alice  ", "Bob", "Charlie"],
            "value": [10.0, 20.0, 30.0],
        }
    )
