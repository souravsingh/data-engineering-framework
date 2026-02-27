"""Bronze layer: raw ingestion of source DataFrames."""

import logging

import pandas as pd

from src.config.schema import MedallionLayerConfig

logger = logging.getLogger(__name__)


class BronzeLayer:
    """Writes raw source data to the Bronze storage layer."""

    def __init__(self, config: MedallionLayerConfig) -> None:
        """Initialise with the bronze layer configuration.

        Args:
            config: Medallion layer configuration (path, format, options).
        """
        self.config = config

    def ingest(self, data: dict[str, pd.DataFrame], writer) -> dict[str, str]:
        """Write raw DataFrames to the Bronze layer.

        Args:
            data: Mapping of table name to its raw DataFrame.
            writer: A :class:`~src.storage.writer.StorageWriter` instance.

        Returns:
            Mapping of table name to the output path it was written to.
        """
        paths: dict[str, str] = {}
        for table_name, df in data.items():
            output_path = f"{self.config.path.rstrip('/')}/{table_name}/"
            logger.info("Bronze: writing table '%s' to %s", table_name, output_path)
            writer.write(df, output_path)
            paths[table_name] = output_path
        return paths
