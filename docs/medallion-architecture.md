# Medallion Architecture

The framework follows the **Medallion** (also called *multi-hop*) architecture,
a data design pattern that organises data into three progressively refined
layers: **Bronze**, **Silver**, and **Gold**.

```
Redshift
   │
   ▼
┌──────────────────────────────────────────────────────────────┐
│  Bronze  (raw)          Silver  (clean)      Gold  (curated) │
│  ──────────────         ──────────────────   ──────────────   │
│  Exact copy of         Deduped, normalised   Business KPIs,   │
│  source data           schema-enforced       aggregations,    │
│  Parquet/CSV           Parquet/Delta         ready for BI     │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
                  Glue / EMR transformation job
                  (complex aggregations & joins)
```

---

## Bronze Layer — Raw Ingestion

**Class:** `src.medallion.bronze.BronzeLayer`

The Bronze layer stores an **exact, unmodified copy** of the data read from
Redshift.  No transformations are applied at this stage.

**What it does:**

- Iterates over every DataFrame in the `dict[str, DataFrame]` produced by
  `RedshiftConnector.read_all_tables()`.
- Writes each DataFrame to `{bronze.path}/{table_name}/` using the configured
  `StorageWriter`.
- Returns a mapping of `table_name → output_path` for lineage tracking.

**Design principle:** The Bronze layer is append-only and should never be
deleted.  It serves as the source of truth if Silver or Gold data needs to be
reprocessed.

**Configuration example:**

```yaml
medallion:
  bronze:
    path: "s3://my-bucket/bronze/orders"
    format: parquet   # raw copy; parquet is the most efficient default
```

---

## Silver Layer — Cleaning & Normalisation

**Class:** `src.medallion.silver.SilverLayer`

The Silver layer applies **lightweight, generic data quality** transformations
that are safe to run on any dataset:

| Transformation | Description |
|---|---|
| Lowercase column names | Ensures consistent naming (`OrderID` → `orderid`) |
| Remove duplicate rows | `DataFrame.drop_duplicates()` |
| Strip whitespace from string columns | Removes leading/trailing spaces from all `object`/`string` columns |

These transformations run in the Python/pandas layer of the pipeline.
Domain-specific business logic (complex joins, aggregations, derived columns)
should live in the **Glue or EMR script** that runs between Silver and Gold.

**Configuration example:**

```yaml
medallion:
  silver:
    path: "s3://my-bucket/silver/orders"
    format: parquet
    # Or use delta for ACID transactions and time-travel:
    # format: delta
```

---

## Gold Layer — Business-Ready Data

**Class:** `src.medallion.gold.GoldLayer`

The Gold layer holds **curated, business-ready** data.  Complex aggregations
and joins are expected to be handled by the Glue or EMR transformation job
configured under `transformation` in the pipeline YAML — the Gold layer simply
persists the output of that job.

`GoldLayer.aggregate()` writes the Silver-layer DataFrames as-is in the local
Python pipeline; when a Glue/EMR job is involved the script itself writes
directly to the Gold path after performing its business logic.

**Configuration example:**

```yaml
medallion:
  gold:
    path: "s3://my-bucket/gold/orders"
    format: parquet
```

---

## Data flow through the pipeline

```python
# pipeline.py — simplified
raw_data    = connector.read_all_tables()          # Redshift → dict of DataFrames
bronze_paths = bronze.ingest(raw_data, writer)      # write to Bronze path
silver_data  = silver.transform(raw_data, writer)   # clean + write to Silver path
gold_data    = gold.aggregate(silver_data, writer)  # write to Gold path
```

For heavy transformations (aggregations, ML feature engineering, multi-table
joins) the Glue or EMR job reads from the Bronze/Silver path, applies the
business logic, and writes the result to the Gold path — entirely outside the
Python orchestrator.

---

## Choosing storage formats per layer

| Layer | Recommended formats | Notes |
|---|---|---|
| Bronze | `parquet` | Compact, splittable; ideal for archival |
| Silver | `parquet`, `delta` | Delta adds ACID + time-travel for reprocessing |
| Gold | `parquet`, `iceberg`, `delta` | Iceberg/Delta enable BI tools to query with snapshot isolation |

`delta`, `iceberg`, and `hudi` require a Spark runtime.  When using these
formats, set them in the medallion layer config and ensure your Glue/EMR script
handles the write using the appropriate Spark connector library.  The Python
`StorageWriter` raises `NotImplementedError` for these formats to make it clear
they must be handled in the Spark job.

---

## Adding custom Silver transformations

To add pipeline-specific Silver transformations, subclass `SilverLayer`:

```python
from src.medallion.silver import SilverLayer
import pandas as pd

class OrdersSilverLayer(SilverLayer):
    def transform(self, data, writer):
        # Apply generic transforms first
        result = super().transform(data, writer)
        # Add pipeline-specific logic
        if "orders" in result:
            df = result["orders"]
            df["amount"] = df["amount"].clip(lower=0)
            result["orders"] = df
        return result
```

Pass the custom layer to the `Pipeline` constructor or instantiate it directly.
