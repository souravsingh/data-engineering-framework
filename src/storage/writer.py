"""Storage writer supporting multiple output formats."""

import logging

import pandas as pd

from src.config.schema import StorageConfig

logger = logging.getLogger(__name__)


class StorageWriter:
    """Writes pandas DataFrames to various storage formats."""

    def __init__(self, config: StorageConfig) -> None:
        """Initialise with a validated StorageConfig.

        Args:
            config: Storage configuration (format, path, options).
        """
        self.config = config

    def write(self, df: pd.DataFrame, output_path: str) -> str:
        """Write a DataFrame to the given path in the configured format.

        Args:
            df: The DataFrame to write.
            output_path: Destination path (local filesystem or S3 URI).

        Returns:
            The ``output_path`` that was written to.

        Raises:
            NotImplementedError: For formats that require Spark (delta, iceberg, hudi).
            ValueError: For unknown format values.
        """
        fmt = self.config.format
        options = self.config.options
        logger.info("Writing %d rows to %s (format=%s)", len(df), output_path, fmt)

        if fmt == "parquet":
            df.to_parquet(output_path, index=False, **options)
        elif fmt == "csv":
            df.to_csv(output_path, index=False, **options)
        elif fmt == "delta":
            raise NotImplementedError("Delta format requires delta-spark library")
        elif fmt == "iceberg":
            raise NotImplementedError("Iceberg format requires Apache Iceberg library")
        elif fmt == "hudi":
            raise NotImplementedError("Hudi format requires Apache Hudi library")
        else:
            raise ValueError(f"Unknown storage format: {fmt}")

        logger.info("Successfully wrote data to %s", output_path)
        return output_path
