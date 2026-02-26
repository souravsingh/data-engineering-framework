"""Tests for RedshiftConnector using mocked psycopg2."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.config.schema import RedshiftConfig, RedshiftSourceTable
from src.connectors.redshift import RedshiftConnector


@pytest.fixture
def redshift_config(sample_redshift_config):
    return RedshiftConfig.model_validate(sample_redshift_config)


class TestRedshiftConnector:
    def test_get_connection(self, redshift_config):
        with patch("src.connectors.redshift.psycopg2.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = RedshiftConnector(redshift_config)
            conn = connector.get_connection()
            mock_connect.assert_called_once_with(
                host="redshift.example.com",
                port=5439,
                dbname="testdb",
                user="admin",
                password="secret",  # noqa: S106
            )
            assert conn is mock_connect.return_value

    def test_read_table(self, redshift_config):
        table = RedshiftSourceTable(name="orders", query="SELECT * FROM orders")
        expected_df = pd.DataFrame({"id": [1, 2], "amount": [100.0, 200.0]})

        with patch("src.connectors.redshift.psycopg2.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            with patch(
                "src.connectors.redshift.pd.read_sql", return_value=expected_df
            ) as mock_read:
                connector = RedshiftConnector(redshift_config)
                df = connector.read_table(table)

        mock_read.assert_called_once_with(table.query, mock_conn)
        pd.testing.assert_frame_equal(df, expected_df)
        mock_conn.close.assert_called_once()

    def test_read_all_tables(self, redshift_config):
        df1 = pd.DataFrame({"order_id": [1]})

        with patch.object(RedshiftConnector, "read_table", return_value=df1) as mock_read:
            connector = RedshiftConnector(redshift_config)
            result = connector.read_all_tables()

        assert "orders" in result
        pd.testing.assert_frame_equal(result["orders"], df1)
        assert mock_read.call_count == len(redshift_config.tables)

    def test_connection_error_raises(self, redshift_config):
        import psycopg2 as _psycopg2

        with patch("src.connectors.redshift.psycopg2.connect") as mock_connect:
            mock_connect.side_effect = _psycopg2.OperationalError("connection refused")
            connector = RedshiftConnector(redshift_config)
            with pytest.raises(_psycopg2.OperationalError):
                connector.get_connection()
