"""Pydantic v2 models for pipeline configuration validation."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, model_validator


class RedshiftSourceTable(BaseModel):
    """Configuration for a single Redshift source table."""

    name: str
    query: str
    schema_name: str = "public"


class RedshiftConfig(BaseModel):
    """Configuration for the Redshift data source."""

    host: str
    port: int = 5439
    database: str
    schema_name: str = "public"
    tables: list[RedshiftSourceTable]
    username: str
    password: str


class GlueConfig(BaseModel):
    """Configuration for an AWS Glue transformation job.

    Attributes:
        job_name: Name of the Glue job in the AWS Glue Data Catalog.
        region: AWS region where the Glue job runs (default ``us-east-1``).
        iam_role: ARN of the IAM role used by the Glue job.
        script_location: S3 URI of the Glue ETL script (``s3://bucket/scripts/job.py``).
        temp_dir: S3 URI Glue uses for temporary files.
        arguments: Extra ``--key value`` arguments passed to every job run.
        redshift_connection_name: Name of a pre-existing AWS Glue Data Catalog connection
            of type ``JDBC`` pointing at Amazon Redshift.  When set, the connection name is
            forwarded to the Glue job as the ``--redshift_connection_name`` argument so the
            PySpark/Glue script can call
            ``glueContext.create_dynamic_frame_from_options(connection_type="redshift", …)``.
            Create the connection with
            :meth:`~src.processors.glue.GlueProcessor.create_redshift_connection`.
        subnet_id: VPC subnet ID used when *creating* a new Glue Redshift connection
            (``PhysicalConnectionRequirements``).  Not required when the connection already
            exists.
        security_group_ids: List of VPC security-group IDs for the Glue Redshift connection's
            physical connection requirements.
        availability_zone: Availability zone for the Glue Redshift connection's physical
            connection requirements.
    """

    job_name: str
    region: str = "us-east-1"
    iam_role: str
    script_location: str
    temp_dir: str
    arguments: dict[str, str] = {}
    redshift_connection_name: str | None = None
    subnet_id: str | None = None
    security_group_ids: list[str] = []
    availability_zone: str | None = None


class EMRConfig(BaseModel):
    """Configuration for an AWS EMR transformation job."""

    cluster_id: str
    region: str = "us-east-1"
    spark_script: str
    arguments: list[str] = []


class TransformationConfig(BaseModel):
    """Configuration for the transformation engine (Glue or EMR)."""

    engine: Literal["glue", "emr"]
    glue: GlueConfig | None = None
    emr: EMRConfig | None = None

    @model_validator(mode="after")
    def validate_engine_config(self) -> "TransformationConfig":  # noqa: UP037
        """Ensure the correct engine config block is present."""
        if self.engine == "glue" and self.glue is None:
            raise ValueError("GlueConfig must be provided when engine is 'glue'")
        if self.engine == "emr" and self.emr is None:
            raise ValueError("EMRConfig must be provided when engine is 'emr'")
        return self


class MedallionLayerConfig(BaseModel):
    """Configuration for a single medallion architecture layer."""

    path: str
    format: Literal["parquet", "csv", "delta", "iceberg", "hudi"] = "parquet"
    options: dict[str, str] = {}


class MedallionConfig(BaseModel):
    """Configuration for all three medallion layers."""

    bronze: MedallionLayerConfig
    silver: MedallionLayerConfig
    gold: MedallionLayerConfig


class StorageConfig(BaseModel):
    """Configuration for the final output storage."""

    format: Literal["parquet", "csv", "delta", "iceberg", "hudi"] = "parquet"
    path: str
    options: dict[str, str] = {}


class PipelineConfig(BaseModel):
    """Top-level pipeline configuration model."""

    name: str
    version: str = "1.0"
    source: RedshiftConfig
    transformation: TransformationConfig
    medallion: MedallionConfig
    storage: StorageConfig
