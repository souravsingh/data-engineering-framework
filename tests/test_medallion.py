"""Tests for the Bronze, Silver, and Gold medallion layers."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.config.schema import MedallionLayerConfig
from src.medallion.bronze import BronzeLayer
from src.medallion.gold import GoldLayer
from src.medallion.silver import SilverLayer


@pytest.fixture
def layer_config(tmp_path):
    return MedallionLayerConfig(path=str(tmp_path), format="parquet")


@pytest.fixture
def mock_writer():
    writer = MagicMock()
    writer.write.side_effect = lambda df, path: path
    return writer


class TestBronzeLayer:
    def test_ingest_calls_writer(self, layer_config, mock_writer, sample_dataframe):
        layer = BronzeLayer(layer_config)
        data = {"orders": sample_dataframe}
        paths = layer.ingest(data, mock_writer)

        mock_writer.write.assert_called_once()
        assert "orders" in paths
        assert "orders" in paths["orders"]

    def test_ingest_multiple_tables(self, layer_config, mock_writer, sample_dataframe):
        layer = BronzeLayer(layer_config)
        data = {"orders": sample_dataframe, "customers": sample_dataframe.copy()}
        paths = layer.ingest(data, mock_writer)

        assert mock_writer.write.call_count == 2
        assert set(paths.keys()) == {"orders", "customers"}


class TestSilverLayer:
    def test_duplicates_removed(self, layer_config, mock_writer):
        df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
        layer = SilverLayer(layer_config)
        result = layer.transform({"tbl": df}, mock_writer)
        assert len(result["tbl"]) == 2

    def test_column_names_lowercased(self, layer_config, mock_writer):
        df = pd.DataFrame({"ID": [1], "Name": ["Alice"], "Value": [10.0]})
        layer = SilverLayer(layer_config)
        result = layer.transform({"tbl": df}, mock_writer)
        assert list(result["tbl"].columns) == ["id", "name", "value"]

    def test_string_whitespace_stripped(self, layer_config, mock_writer, sample_dataframe):
        layer = SilverLayer(layer_config)
        result = layer.transform({"tbl": sample_dataframe}, mock_writer)
        # "  Alice  " should become "Alice"
        assert result["tbl"]["name"].iloc[0] == "Alice"

    def test_writer_called(self, layer_config, mock_writer, sample_dataframe):
        layer = SilverLayer(layer_config)
        layer.transform({"tbl": sample_dataframe}, mock_writer)
        mock_writer.write.assert_called_once()


class TestGoldLayer:
    def test_aggregate_calls_writer(self, layer_config, mock_writer, sample_dataframe):
        layer = GoldLayer(layer_config)
        result = layer.aggregate({"tbl": sample_dataframe}, mock_writer)

        mock_writer.write.assert_called_once()
        assert "tbl" in result

    def test_aggregate_returns_unchanged_data(self, layer_config, mock_writer, sample_dataframe):
        layer = GoldLayer(layer_config)
        result = layer.aggregate({"tbl": sample_dataframe}, mock_writer)
        pd.testing.assert_frame_equal(result["tbl"], sample_dataframe)
