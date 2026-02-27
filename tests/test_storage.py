"""Tests for StorageWriter covering all supported output formats."""

import pandas as pd
import pytest

from src.config.schema import StorageConfig
from src.storage.writer import StorageWriter


@pytest.fixture
def sample_df():
    return pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})


class TestStorageWriter:
    def test_write_parquet(self, tmp_path, sample_df):
        output = str(tmp_path / "data.parquet")
        config = StorageConfig(format="parquet", path=str(tmp_path))
        writer = StorageWriter(config)
        result = writer.write(sample_df, output)

        assert result == output
        loaded = pd.read_parquet(output)
        pd.testing.assert_frame_equal(loaded, sample_df)

    def test_write_csv(self, tmp_path, sample_df):
        output = str(tmp_path / "data.csv")
        config = StorageConfig(format="csv", path=str(tmp_path))
        writer = StorageWriter(config)
        result = writer.write(sample_df, output)

        assert result == output
        loaded = pd.read_csv(output)
        pd.testing.assert_frame_equal(loaded, sample_df)

    def test_write_delta_not_implemented(self, tmp_path, sample_df):
        config = StorageConfig(format="delta", path=str(tmp_path))
        writer = StorageWriter(config)
        with pytest.raises(NotImplementedError, match="delta-spark"):
            writer.write(sample_df, str(tmp_path / "out"))

    def test_write_iceberg_not_implemented(self, tmp_path, sample_df):
        config = StorageConfig(format="iceberg", path=str(tmp_path))
        writer = StorageWriter(config)
        with pytest.raises(NotImplementedError, match="Apache Iceberg"):
            writer.write(sample_df, str(tmp_path / "out"))

    def test_write_hudi_not_implemented(self, tmp_path, sample_df):
        config = StorageConfig(format="hudi", path=str(tmp_path))
        writer = StorageWriter(config)
        with pytest.raises(NotImplementedError, match="Apache Hudi"):
            writer.write(sample_df, str(tmp_path / "out"))

    def test_write_returns_output_path(self, tmp_path, sample_df):
        output = str(tmp_path / "data.parquet")
        config = StorageConfig(format="parquet", path=str(tmp_path))
        writer = StorageWriter(config)
        assert writer.write(sample_df, output) == output
