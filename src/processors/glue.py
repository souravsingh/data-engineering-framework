"""AWS Glue processor for running ETL transformation jobs.

This module provides :class:`GlueProcessor`, which:

* Starts and monitors AWS Glue job runs.
* Creates and retrieves AWS Glue Data Catalog connections that point at Amazon
  Redshift (JDBC type).  Using a named Glue connection instead of hard-coding
  JDBC credentials in job arguments keeps credentials out of job runs and lets
  Glue manage the VPC/network path to your Redshift cluster.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import boto3

from src.config.schema import GlueConfig

from .base import BaseProcessor

if TYPE_CHECKING:
    from src.config.schema import RedshiftConfig

logger = logging.getLogger(__name__)

_TERMINAL_STATES = {"SUCCEEDED", "FAILED", "STOPPED", "ERROR", "TIMEOUT"}


class GlueProcessor(BaseProcessor):
    """Runs and monitors AWS Glue ETL jobs."""

    def __init__(self, config: GlueConfig) -> None:
        """Initialise with a validated GlueConfig.

        Args:
            config: Glue job configuration.
        """
        self.config = config
        self._client = boto3.client("glue", region_name=config.region)

    def run_job(self, **kwargs) -> dict:
        """Start a Glue job run.

        If :attr:`~src.config.schema.GlueConfig.redshift_connection_name` is set
        in the config, it is automatically injected as the
        ``--redshift_connection_name`` job argument so the Glue ETL script can
        retrieve the connection name at runtime (e.g. to pass to
        ``glueContext.create_dynamic_frame_from_options``).

        Args:
            **kwargs: Additional ``--key value`` arguments merged on top of the
                arguments defined in the config.  Caller-supplied values take
                precedence over config values.

        Returns:
            Dict with ``job_run_id`` and initial ``status`` (``"STARTING"``)
            keys.
        """
        arguments = {**self.config.arguments, **kwargs}
        if self.config.redshift_connection_name:
            arguments.setdefault(
                "--redshift_connection_name", self.config.redshift_connection_name
            )
        logger.info("Starting Glue job '%s'", self.config.job_name)
        response = self._client.start_job_run(
            JobName=self.config.job_name,
            Arguments=arguments,
        )
        job_run_id = response["JobRunId"]
        logger.info("Glue job '%s' started with run ID %s", self.config.job_name, job_run_id)
        return {"job_run_id": job_run_id, "status": "STARTING"}

    def get_job_status(self, job_run_id: str) -> str:
        """Return the current status of a Glue job run.

        Args:
            job_run_id: The Glue job run identifier.

        Returns:
            Status string (e.g. ``"SUCCEEDED"``, ``"RUNNING"``).
        """
        response = self._client.get_job_run(
            JobName=self.config.job_name,
            RunId=job_run_id,
        )
        return response["JobRun"]["JobRunState"]

    def wait_for_completion(self, job_run_id: str, poll_interval: int = 30) -> str:
        """Poll until the Glue job run reaches a terminal state.

        Args:
            job_run_id: The Glue job run identifier.
            poll_interval: Seconds to wait between status polls.

        Returns:
            The final status string.

        Raises:
            RuntimeError: If the job ends in a failed/stopped state.
        """
        logger.info("Waiting for Glue job run %s to complete", job_run_id)
        while True:
            status = self.get_job_status(job_run_id)
            logger.info("Glue job run %s status: %s", job_run_id, status)
            if status in _TERMINAL_STATES:
                if status != "SUCCEEDED":
                    raise RuntimeError(f"Glue job run {job_run_id} ended with status {status}")
                return status
            time.sleep(poll_interval)

    # ------------------------------------------------------------------
    # Glue Data Catalog Redshift connection management
    # ------------------------------------------------------------------

    def create_redshift_connection(
        self,
        redshift_config: RedshiftConfig,
        *,
        enforce_ssl: bool = False,
    ) -> str:
        """Create an AWS Glue Data Catalog connection that points at Redshift.

        The connection is of type ``JDBC`` and uses the host, port, database,
        username, and password from *redshift_config*.  Physical connection
        requirements (subnet, security groups, AZ) are taken from the
        :class:`~src.config.schema.GlueConfig` fields ``subnet_id``,
        ``security_group_ids``, and ``availability_zone`` when provided.

        After creation the Glue job must be associated with the connection
        (set in the job definition via ``Connections``).  The connection name is
        automatically passed to ``run_job`` as the ``--redshift_connection_name``
        argument so PySpark scripts can reference it at runtime.

        Example Glue script usage::

            args = getResolvedOptions(sys.argv, ["redshift_connection_name"])
            df = glueContext.create_dynamic_frame_from_options(
                connection_type="redshift",
                connection_options={
                    "url": "jdbc:redshift://host:5439/dbname",
                    "dbtable": "public.orders",
                    "redshiftTmpDir": args["TempDir"],
                    "aws_iam_role": args["iam_role"],
                },
                transformation_ctx="source",
            )

        Args:
            redshift_config: Redshift source configuration supplying the host,
                port, database name, and credentials for the JDBC URL.
            enforce_ssl: When ``True``, sets ``JDBC_ENFORCE_SSL`` to ``"true"``
                in the connection properties.

        Returns:
            The name of the created Glue connection
            (:attr:`~src.config.schema.GlueConfig.redshift_connection_name`).

        Raises:
            ValueError: If ``redshift_connection_name`` is not set in the
                :class:`~src.config.schema.GlueConfig`.
            botocore.exceptions.ClientError: If the AWS Glue API call fails
                (e.g. the connection already exists or IAM permissions are
                insufficient).
        """
        if not self.config.redshift_connection_name:
            raise ValueError(
                "GlueConfig.redshift_connection_name must be set before calling "
                "create_redshift_connection()"
            )

        jdbc_url = (
            f"jdbc:redshift://{redshift_config.host}:{redshift_config.port}"
            f"/{redshift_config.database}"
        )
        connection_input: dict = {
            "Name": self.config.redshift_connection_name,
            "Description": (
                f"Glue JDBC connection to Redshift database "
                f"'{redshift_config.database}' on {redshift_config.host}"
            ),
            "ConnectionType": "JDBC",
            "ConnectionProperties": {
                "JDBC_CONNECTION_URL": jdbc_url,
                "USERNAME": redshift_config.username,
                "PASSWORD": redshift_config.password,
                "JDBC_ENFORCE_SSL": "true" if enforce_ssl else "false",
            },
        }

        physical: dict = {}
        if self.config.subnet_id:
            physical["SubnetId"] = self.config.subnet_id
        if self.config.security_group_ids:
            physical["SecurityGroupIdList"] = self.config.security_group_ids
        if self.config.availability_zone:
            physical["AvailabilityZone"] = self.config.availability_zone
        if physical:
            connection_input["PhysicalConnectionRequirements"] = physical

        logger.info(
            "Creating Glue Redshift connection '%s' (host=%s db=%s)",
            self.config.redshift_connection_name,
            redshift_config.host,
            redshift_config.database,
        )
        self._client.create_connection(ConnectionInput=connection_input)
        logger.info(
            "Glue Redshift connection '%s' created successfully",
            self.config.redshift_connection_name,
        )
        return self.config.redshift_connection_name

    def get_redshift_connection(self, connection_name: str) -> dict:
        """Retrieve details of a Glue Data Catalog connection.

        Args:
            connection_name: Name of the Glue connection to retrieve.

        Returns:
            The connection detail dict as returned by the Glue API, containing
            keys such as ``Name``, ``ConnectionType``, ``ConnectionProperties``,
            and ``PhysicalConnectionRequirements``.

        Raises:
            botocore.exceptions.ClientError: If the connection does not exist or
                the caller lacks ``glue:GetConnection`` permission.
        """
        logger.info("Retrieving Glue connection '%s'", connection_name)
        response = self._client.get_connection(Name=connection_name)
        return response["Connection"]
