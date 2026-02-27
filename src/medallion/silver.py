"""Silver layer: cleaned and lightly transformed DataFrames."""

import logging

import pandas as pd

from src.config.schema import MedallionLayerConfig

logger = logging.getLogger(__name__)


class SilverLayer:
    """Applies basic data quality transformations and writes to the Silver layer."""

    def __init__(self, config: MedallionLayerConfig) -> None:
        """Initialise with the silver layer configuration.

        Args:
            config: Medallion layer configuration (path, format, options).
        """
        self.config = config

    def transform(self, data: dict[str, pd.DataFrame], writer) -> dict[str, pd.DataFrame]:
        """Clean and transform raw DataFrames for the Silver layer.

        Transformations applied:
        - Remove duplicate rows.
        - Lowercase all column names.
        - Strip leading/trailing whitespace from string columns.

        Args:
            data: Mapping of table name to its raw (Bronze) DataFrame.
            writer: A :class:`~src.storage.writer.StorageWriter` instance.

        Returns:
            Mapping of table name to the cleaned DataFrame.
        """
        transformed: dict[str, pd.DataFrame] = {}
        for table_name, df in data.items():
            logger.info("Silver: transforming table '%s' (%d rows)", table_name, len(df))

            # Lowercase column names
            df = df.copy()
            df.columns = [col.lower() for col in df.columns]

            # Remove duplicates
            before = len(df)
            df = df.drop_duplicates()
            logger.debug(
                "Silver: removed %d duplicate rows from '%s'", before - len(df), table_name
            )

            # Strip whitespace from string columns
            for col in df.select_dtypes(include=["object", "string"]).columns:
                df[col] = df[col].str.strip()

            output_path = f"{self.config.path.rstrip('/')}/{table_name}/"
            logger.info("Silver: writing table '%s' to %s", table_name, output_path)
            writer.write(df, output_path)

            transformed[table_name] = df
        return transformed
