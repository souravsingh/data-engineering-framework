"""Redshift connector for reading source tables into pandas DataFrames."""

import logging

import pandas as pd
import psycopg2

from src.config.schema import RedshiftConfig, RedshiftSourceTable

logger = logging.getLogger(__name__)


class RedshiftConnector:
    """Connects to Amazon Redshift and reads data into DataFrames."""

    def __init__(self, config: RedshiftConfig) -> None:
        """Initialise with a validated RedshiftConfig.

        Args:
            config: Redshift connection configuration.
        """
        self.config = config

    def get_connection(self):
        """Create and return a psycopg2 connection to Redshift.

        Returns:
            An open psycopg2 connection.

        Raises:
            psycopg2.OperationalError: If the connection cannot be established.
        """
        try:
            conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.database,
                user=self.config.username,
                password=self.config.password,
            )
            logger.info(
                "Connected to Redshift host=%s db=%s", self.config.host, self.config.database
            )
            return conn
        except psycopg2.OperationalError as exc:
            logger.error("Failed to connect to Redshift: %s", exc)
            raise

    def read_table(self, table: RedshiftSourceTable) -> pd.DataFrame:
        """Execute the table query and return results as a DataFrame.

        Args:
            table: Source table configuration including the SQL query.

        Returns:
            A pandas DataFrame with the query results.
        """
        logger.info("Reading table '%s' from Redshift", table.name)
        conn = self.get_connection()
        try:
            df = pd.read_sql(table.query, conn)
            logger.info("Read %d rows from table '%s'", len(df), table.name)
            return df
        finally:
            conn.close()

    def read_all_tables(self) -> dict[str, pd.DataFrame]:
        """Read every table defined in the config.

        Returns:
            A mapping of table name to its DataFrame.
        """
        result: dict[str, pd.DataFrame] = {}
        for table in self.config.tables:
            result[table.name] = self.read_table(table)
        return result
