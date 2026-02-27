# Data Engineering Framework

A production-ready Python framework for building data pipelines that read from
**Amazon Redshift**, transform data via **AWS Glue** or **AWS EMR**, follow the
**Medallion architecture** (Bronze → Silver → Gold), and write output in
**Parquet, CSV, Delta, Iceberg, or Hudi**.

Each pipeline is driven by a single **YAML configuration file** — no code
changes are needed to add a new pipeline or swap the transformation engine.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Pipeline Configuration](#pipeline-configuration)
- [Glue Redshift Connection](#glue-redshift-connection)
- [Medallion Architecture](#medallion-architecture)
- [Storage Formats](#storage-formats)
- [Running Pipelines](#running-pipelines)
- [Development](#development)
- [CI/CD](#cicd)
- [Documentation](#documentation)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         Pipeline YAML Config                             │
│  source (Redshift) │ transformation (Glue/EMR) │ medallion │ storage     │
└──────────────────────────────────────────────────────────────────────────┘
          │                       │                     │
          ▼                       ▼                     ▼
  ┌───────────────┐    ┌─────────────────────┐   ┌──────────────┐
  │   Redshift    │    │  AWS Glue  │ AWS EMR │   │   Storage    │
  │  Connector    │    │  Processor │Processor│   │   Writer     │
  └──────┬────────┘    └─────────────────────┘   └──────────────┘
         │
         ▼
  ┌─────────────────────────────────────────────────────┐
  │              Medallion Architecture                  │
  │  ┌──────────┐   ┌──────────┐   ┌──────────────┐    │
  │  │  Bronze  │──▶│  Silver  │──▶│     Gold     │    │
  │  │ Raw data │   │ Cleaned  │   │  Aggregated  │    │
  │  └──────────┘   └──────────┘   └──────────────┘    │
  └─────────────────────────────────────────────────────┘
```

---

## Quick Start

### 1. Install

```bash
pip install -e .
# For development (tests, linting, security scan):
pip install -e ".[dev]"
```

### 2. Set environment variables

```bash
export REDSHIFT_HOST=my-cluster.abc123.us-east-1.redshift.amazonaws.com
export REDSHIFT_DB=mydb
export REDSHIFT_USER=admin
export REDSHIFT_PASSWORD=supersecret
export S3_BUCKET=my-data-lake-bucket
export GLUE_IAM_ROLE=arn:aws:iam::123456789012:role/GlueServiceRole
export GLUE_REDSHIFT_CONNECTION_NAME=my-redshift-conn
export GLUE_SUBNET_ID=subnet-0abc1234
export GLUE_SECURITY_GROUP_ID=sg-0abc1234
export GLUE_AZ=us-east-1a
```

### 3. (One-time) Create the Glue Redshift connection

```python
from src.config.loader import ConfigLoader
from src.config.schema import RedshiftConfig
from src.processors.glue import GlueProcessor

loader = ConfigLoader()
config = loader.load("configs/pipelines/example_orders_pipeline.yaml")

processor = GlueProcessor(config.transformation.glue)
processor.create_redshift_connection(config.source)
```

### 4. Run a pipeline

```python
from src.config.loader import ConfigLoader
from src.pipeline import Pipeline

config = ConfigLoader().load("configs/pipelines/example_orders_pipeline.yaml")
pipeline = Pipeline(config)
summary = pipeline.run()
print(summary)
```

---

## Project Structure

```
data-engineering-framework/
├── configs/
│   └── pipelines/                  # One YAML file per pipeline
│       ├── example_orders_pipeline.yaml
│       └── example_customers_pipeline.yaml
├── docs/
│   ├── configuration.md            # Full YAML config reference
│   ├── glue-redshift-connection.md # Setting up Glue Redshift connections
│   ├── medallion-architecture.md   # Medallion layers explained
│   └── contributing.md             # Development guide
├── src/
│   ├── config/
│   │   ├── loader.py               # YAML loader with ${ENV_VAR} interpolation
│   │   └── schema.py               # Pydantic v2 config models
│   ├── connectors/
│   │   └── redshift.py             # psycopg2-based Redshift connector
│   ├── medallion/
│   │   ├── bronze.py               # Raw ingestion layer
│   │   ├── silver.py               # Cleaning & transformation layer
│   │   └── gold.py                 # Aggregation layer
│   ├── processors/
│   │   ├── base.py                 # Abstract base processor
│   │   ├── glue.py                 # AWS Glue processor + connection management
│   │   └── emr.py                  # AWS EMR Spark step processor
│   ├── storage/
│   │   └── writer.py               # Multi-format storage writer
│   └── pipeline.py                 # Pipeline orchestrator
├── tests/                          # pytest test suite (99% coverage)
├── azure-pipelines.yml             # Azure DevOps CI/CD pipeline
├── pyproject.toml
├── requirements.txt
└── requirements-dev.txt
```

---

## Pipeline Configuration

Each pipeline has its own YAML file under `configs/pipelines/`.  Values can
reference environment variables with `${VAR_NAME}` syntax — unset variables
are left as-is (the placeholder is preserved).

```yaml
name: orders_pipeline
version: "1.0"

source:                              # Redshift source
  host: "${REDSHIFT_HOST}"
  port: 5439
  database: "${REDSHIFT_DB}"
  username: "${REDSHIFT_USER}"
  password: "${REDSHIFT_PASSWORD}"
  tables:
    - name: orders
      query: "SELECT * FROM public.orders"

transformation:
  engine: glue                       # "glue" or "emr"
  glue:
    job_name: orders-etl-job
    iam_role: "${GLUE_IAM_ROLE}"
    script_location: "s3://${S3_BUCKET}/scripts/orders_transform.py"
    temp_dir: "s3://${S3_BUCKET}/temp/"
    redshift_connection_name: "${GLUE_REDSHIFT_CONNECTION_NAME}"  # Glue connection
    subnet_id: "${GLUE_SUBNET_ID}"
    security_group_ids: ["${GLUE_SECURITY_GROUP_ID}"]
    availability_zone: "${GLUE_AZ}"

medallion:
  bronze: { path: "s3://${S3_BUCKET}/bronze/orders", format: parquet }
  silver: { path: "s3://${S3_BUCKET}/silver/orders", format: parquet }
  gold:   { path: "s3://${S3_BUCKET}/gold/orders",   format: parquet }

storage:
  format: parquet                    # parquet | csv | delta | iceberg | hudi
  path: "s3://${S3_BUCKET}/output/orders"
  options:
    compression: snappy
```

See [`docs/configuration.md`](docs/configuration.md) for the full field
reference.

---

## Glue Redshift Connection

Instead of passing raw JDBC credentials as Glue job arguments, this framework
uses a named **AWS Glue Data Catalog connection** of type `JDBC`.  This keeps
credentials out of job run history and lets Glue manage VPC routing to your
Redshift cluster.

```python
from src.processors.glue import GlueProcessor
from src.config.schema import GlueConfig, RedshiftConfig

glue_cfg = GlueConfig(
    job_name="my-job",
    iam_role="arn:aws:iam::123456789012:role/GlueRole",
    script_location="s3://bucket/scripts/job.py",
    temp_dir="s3://bucket/temp/",
    redshift_connection_name="my-redshift-conn",
    subnet_id="subnet-0abc1234",
    security_group_ids=["sg-0abc1234"],
    availability_zone="us-east-1a",
)
redshift_cfg = RedshiftConfig(
    host="cluster.abc.redshift.amazonaws.com",
    database="mydb",
    username="admin",
    password="secret",
    tables=[],
)

processor = GlueProcessor(glue_cfg)
processor.create_redshift_connection(redshift_cfg)   # creates the connection once
processor.run_job()   # --redshift_connection_name is injected automatically
```

See [`docs/glue-redshift-connection.md`](docs/glue-redshift-connection.md) for
the full guide.

---

## Medallion Architecture

| Layer  | Class         | What happens                                              |
|--------|---------------|-----------------------------------------------------------|
| Bronze | `BronzeLayer` | Raw DataFrames written as-is from Redshift                |
| Silver | `SilverLayer` | Deduplication, lowercase column names, whitespace strip   |
| Gold   | `GoldLayer`   | Business-ready data (aggregations handled by Glue/EMR)    |

See [`docs/medallion-architecture.md`](docs/medallion-architecture.md).

---

## Storage Formats

| Format  | Local / pandas | Notes                                          |
|---------|---------------|------------------------------------------------|
| parquet | ✅            | Default; uses PyArrow via pandas               |
| csv     | ✅            | Plain CSV via pandas                           |
| delta   | ⚙️ Spark only  | Requires `delta-spark`; handled in Glue/EMR   |
| iceberg | ⚙️ Spark only  | Requires Apache Iceberg; handled in Glue/EMR  |
| hudi    | ⚙️ Spark only  | Requires Apache Hudi; handled in Glue/EMR     |

---

## Running Pipelines

```python
from src.config.loader import ConfigLoader
from src.pipeline import Pipeline

# Load and validate config
config = ConfigLoader().load("configs/pipelines/example_orders_pipeline.yaml")

# Instantiate and run
pipeline = Pipeline(config)
summary = pipeline.run()
# {
#   "status": "success",
#   "pipeline": "orders_pipeline",
#   "bronze_paths": {"orders": "s3://…/bronze/orders/orders/"},
#   "silver_tables": ["orders"],
#   "gold_tables": ["orders"],
# }
```

---

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests with coverage
pytest tests/

# Lint
ruff check src/ tests/

# Type check
mypy src/

# Security scan
bandit -r src/ --skip B101
```

See [`docs/contributing.md`](docs/contributing.md) for the full guide.

---

## CI/CD

`azure-pipelines.yml` defines a three-stage Azure DevOps pipeline triggered on
every push/PR to `main` or `develop`:

| Stage         | Steps                                                    |
|---------------|----------------------------------------------------------|
| **Test**      | `ruff` lint → `mypy` → `pytest` + coverage → publish    |
| **SecurityScan** | `bandit` SAST → publish XML report                  |
| **Build**     | `python -m build` → publish `dist/` artifacts           |

---

## Documentation

| Document | Description |
|---|---|
| [`docs/configuration.md`](docs/configuration.md) | Full YAML config field reference |
| [`docs/glue-redshift-connection.md`](docs/glue-redshift-connection.md) | Step-by-step Glue Redshift connection guide |
| [`docs/medallion-architecture.md`](docs/medallion-architecture.md) | Medallion layers in detail |
| [`docs/contributing.md`](docs/contributing.md) | Development setup and contribution guide |
