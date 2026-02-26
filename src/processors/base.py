"""Abstract base class for transformation processors."""

from abc import ABC, abstractmethod


class BaseProcessor(ABC):
    """Abstract base for Glue and EMR transformation processors."""

    @abstractmethod
    def run_job(self, **kwargs) -> dict:
        """Run the transformation job.

        Returns:
            A dict containing at minimum a job/step identifier and initial status.
        """
