"""Main pipeline orchestrator."""

import logging

from src.config.schema import PipelineConfig
from src.connectors.redshift import RedshiftConnector
from src.medallion.bronze import BronzeLayer
from src.medallion.gold import GoldLayer
from src.medallion.silver import SilverLayer
from src.processors.emr import EMRProcessor
from src.processors.glue import GlueProcessor
from src.storage.writer import StorageWriter

logger = logging.getLogger(__name__)


class Pipeline:
    """Orchestrates end-to-end data movement through the medallion layers."""

    def __init__(self, config: PipelineConfig) -> None:
        """Initialise the pipeline and its components from a validated config.

        Args:
            config: Fully validated PipelineConfig instance.
        """
        self.config = config
        self.connector = RedshiftConnector(config.source)
        self.writer = StorageWriter(config.storage)
        self.bronze = BronzeLayer(config.medallion.bronze)
        self.silver = SilverLayer(config.medallion.silver)
        self.gold = GoldLayer(config.medallion.gold)

        if config.transformation.engine == "glue":
            self.processor = GlueProcessor(config.transformation.glue)  # type: ignore[arg-type]
        else:
            self.processor = EMRProcessor(config.transformation.emr)  # type: ignore[arg-type]

    def run(self) -> dict:
        """Execute the full pipeline.

        Steps:
            1. Read raw data from Redshift.
            2. Ingest raw data into the Bronze layer.
            3. Clean and transform data into the Silver layer.
            4. Aggregate and write data into the Gold layer.

        Returns:
            A summary dict containing ``status``, ``pipeline``, and layer path maps.
        """
        logger.info("Pipeline '%s' starting", self.config.name)

        # Step 1: read from Redshift
        raw_data = self.connector.read_all_tables()

        # Step 2: Bronze layer — raw ingestion
        bronze_paths = self.bronze.ingest(raw_data, self.writer)

        # Step 3: Silver layer — cleaning & transformation
        silver_data = self.silver.transform(raw_data, self.writer)

        # Step 4: Gold layer — aggregation
        gold_data = self.gold.aggregate(silver_data, self.writer)

        summary = {
            "status": "success",
            "pipeline": self.config.name,
            "bronze_paths": bronze_paths,
            "silver_tables": list(silver_data.keys()),
            "gold_tables": list(gold_data.keys()),
        }
        logger.info("Pipeline '%s' completed successfully", self.config.name)
        return summary
