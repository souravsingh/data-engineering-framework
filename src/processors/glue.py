"""AWS Glue processor for running ETL transformation jobs."""

import logging
import time

import boto3

from src.config.schema import GlueConfig

from .base import BaseProcessor

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

        Args:
            **kwargs: Additional arguments merged into the job arguments.

        Returns:
            Dict with ``job_run_id`` and ``status`` keys.
        """
        arguments = {**self.config.arguments, **kwargs}
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
