"""Gold layer: aggregated, business-ready DataFrames."""

import logging

import pandas as pd

from src.config.schema import MedallionLayerConfig

logger = logging.getLogger(__name__)


class GoldLayer:
    """Writes aggregated, business-ready data to the Gold storage layer."""

    def __init__(self, config: MedallionLayerConfig) -> None:
        """Initialise with the gold layer configuration.

        Args:
            config: Medallion layer configuration (path, format, options).
        """
        self.config = config

    def aggregate(self, data: dict[str, pd.DataFrame], writer) -> dict[str, pd.DataFrame]:
        """Write Silver-layer DataFrames to the Gold layer.

        In production, domain-specific aggregations and business logic would be
        applied here (or handled upstream by the Glue/EMR transformation engine).
        This method writes the provided data as-is so that the pipeline
        orchestrator can pass pre-aggregated results from the transformation step.

        Args:
            data: Mapping of table name to its Silver-layer DataFrame.
            writer: A :class:`~src.storage.writer.StorageWriter` instance.

        Returns:
            Mapping of table name to the Gold-layer DataFrame (unchanged).
        """
        result: dict[str, pd.DataFrame] = {}
        for table_name, df in data.items():
            output_path = f"{self.config.path.rstrip('/')}/{table_name}/"
            logger.info("Gold: writing table '%s' to %s", table_name, output_path)
            writer.write(df, output_path)
            result[table_name] = df
        return result
