"""AWS EMR processor for running Spark transformation steps."""

import logging
import time

import boto3

from src.config.schema import EMRConfig

from .base import BaseProcessor

logger = logging.getLogger(__name__)

_TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"}


class EMRProcessor(BaseProcessor):
    """Adds and monitors Spark steps on an AWS EMR cluster."""

    def __init__(self, config: EMRConfig) -> None:
        """Initialise with a validated EMRConfig.

        Args:
            config: EMR cluster and step configuration.
        """
        self.config = config
        self._client = boto3.client("emr", region_name=config.region)

    def run_job(self, **kwargs) -> dict:
        """Add a Spark step to the EMR cluster.

        Args:
            **kwargs: Unused; reserved for future extension.

        Returns:
            Dict with ``step_id`` and ``status`` keys.
        """
        logger.info("Adding Spark step to EMR cluster '%s'", self.config.cluster_id)
        response = self._client.add_job_flow_steps(
            JobFlowId=self.config.cluster_id,
            Steps=[
                {
                    "Name": "spark-step",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["spark-submit", self.config.spark_script, *self.config.arguments],
                    },
                }
            ],
        )
        step_id = response["StepIds"][0]
        logger.info("EMR step %s added to cluster %s", step_id, self.config.cluster_id)
        return {"step_id": step_id, "status": "PENDING"}

    def get_step_status(self, step_id: str) -> str:
        """Return the current status of an EMR step.

        Args:
            step_id: The EMR step identifier.

        Returns:
            Status string (e.g. ``"COMPLETED"``, ``"RUNNING"``).
        """
        response = self._client.list_steps(
            ClusterId=self.config.cluster_id,
            StepIds=[step_id],
        )
        return response["Steps"][0]["Status"]["State"]

    def wait_for_completion(self, step_id: str, poll_interval: int = 30) -> str:
        """Poll until the EMR step reaches a terminal state.

        Args:
            step_id: The EMR step identifier.
            poll_interval: Seconds to wait between status polls.

        Returns:
            The final status string.

        Raises:
            RuntimeError: If the step ends in a failed/cancelled state.
        """
        logger.info("Waiting for EMR step %s to complete", step_id)
        while True:
            status = self.get_step_status(step_id)
            logger.info("EMR step %s status: %s", step_id, status)
            if status in _TERMINAL_STATES:
                if status != "COMPLETED":
                    raise RuntimeError(f"EMR step {step_id} ended with status {status}")
                return status
            time.sleep(poll_interval)
