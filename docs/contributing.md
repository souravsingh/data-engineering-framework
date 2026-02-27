# Contributing Guide

This document explains how to set up a local development environment, run
tests, check code style, and run security scans — mirroring exactly what the
Azure DevOps CI/CD pipeline executes on every pull request.

---

## Prerequisites

- Python 3.9 or later (3.11 recommended — matches CI)
- `git`
- An AWS account is **not** required for local development; all AWS calls are
  mocked in the test suite.

---

## 1. Clone and install

```bash
git clone https://github.com/souravsingh/data-engineering-framework.git
cd data-engineering-framework

# Install the package in editable mode plus all dev dependencies
pip install -e ".[dev]"
```

The `[dev]` extra installs:

| Package | Purpose |
|---|---|
| `pytest` | Test runner |
| `pytest-cov` | Coverage reporting |
| `pytest-mock` | Mock helpers for pytest |
| `ruff` | Linter and import sorter |
| `mypy` | Static type checker |
| `bandit` | Security vulnerability scanner |
| `moto[s3,glue,emr]` | AWS service mocks for tests |
| `types-pyyaml` | Type stubs for PyYAML |

---

## 2. Run tests

```bash
pytest tests/
```

This produces:

- A terminal coverage summary (target: ≥ 95 %)
- `coverage.xml` — consumed by Azure DevOps `PublishCodeCoverageResults`
- `htmlcov/` — open `htmlcov/index.html` in a browser for the line-by-line report

Run a specific test file or test class:

```bash
pytest tests/test_processors.py
pytest tests/test_processors.py::TestGlueProcessor
pytest tests/test_processors.py::TestGlueProcessor::test_create_redshift_connection
```

Run with verbose output:

```bash
pytest tests/ -v
```

---

## 3. Lint with Ruff

```bash
ruff check src/ tests/
```

To auto-fix safe issues:

```bash
ruff check src/ tests/ --fix
```

Ruff is configured in `pyproject.toml` (`[tool.ruff]`).  The enabled rule
sets are:

| Code | Description |
|---|---|
| `E`, `W` | pycodestyle errors and warnings |
| `F` | pyflakes (undefined names, unused imports, …) |
| `I` | isort (import ordering) |
| `N` | pep8-naming |
| `UP` | pyupgrade (modern Python syntax) |
| `S` | flake8-bandit (security) |
| `B` | flake8-bugbear |

`S101` (use of `assert`) is suppressed for the test suite.

---

## 4. Type check with mypy

```bash
mypy src/
```

mypy is configured in `pyproject.toml` (`[tool.mypy]`).  `strict` mode is
disabled to allow gradual adoption; `ignore_missing_imports = true` suppresses
errors for third-party libraries that lack stubs.

---

## 5. Security scan with Bandit

```bash
bandit -r src/ --skip B101
```

`B101` (assert used) is skipped because `assert` is legitimate in production
dataclass validation.  The `tests/` directory is excluded.

To produce an XML report (as CI does):

```bash
bandit -r src/ --skip B101 -f xml -o bandit-results.xml
```

---

## 6. Project layout

```
src/
  config/          ConfigLoader + Pydantic models
  connectors/      RedshiftConnector (psycopg2)
  medallion/       Bronze, Silver, Gold layers
  processors/      GlueProcessor, EMRProcessor
  storage/         StorageWriter (parquet, csv, …)
  pipeline.py      Pipeline orchestrator
configs/
  pipelines/       One YAML file per pipeline
docs/              Markdown documentation
tests/             pytest test suite
  conftest.py      Shared fixtures
  test_config.py
  test_connectors.py
  test_processors.py
  test_medallion.py
  test_storage.py
  test_pipeline.py
```

---

## 7. Adding a new pipeline

1. Copy an existing config file:

   ```bash
   cp configs/pipelines/example_orders_pipeline.yaml \
      configs/pipelines/my_new_pipeline.yaml
   ```

2. Edit `my_new_pipeline.yaml` — change `name`, `source.tables`,
   `transformation`, and `medallion`/`storage` paths.

3. If using Glue with a Redshift connection, create the connection once:

   ```python
   from src.config.loader import ConfigLoader
   from src.processors.glue import GlueProcessor

   cfg = ConfigLoader().load("configs/pipelines/my_new_pipeline.yaml")
   GlueProcessor(cfg.transformation.glue).create_redshift_connection(cfg.source)
   ```

4. Run:

   ```python
   from src.pipeline import Pipeline
   Pipeline(cfg).run()
   ```

No code changes are required.

---

## 8. Adding a new transformation engine

1. Add a new config model in `src/config/schema.py` (similar to `GlueConfig`).
2. Add `my_engine: MyEngineConfig | None = None` to `TransformationConfig`.
3. Update the `validate_engine_config` validator.
4. Create `src/processors/my_engine.py` extending `BaseProcessor`.
5. Update `Pipeline.__init__` to instantiate the new processor.
6. Add tests in `tests/test_processors.py`.

---

## 9. CI/CD pipeline

The `azure-pipelines.yml` file defines three sequential stages:

```
Test → SecurityScan → Build
```

| Stage | Commands |
|---|---|
| **Test** | `ruff check` · `mypy src/` · `pytest tests/` |
| **SecurityScan** | `bandit -r src/` |
| **Build** | `python -m build` → publishes `dist/` artifact |

All stages run on `ubuntu-latest` with Python 3.11.  Test results and
coverage are published as pipeline artifacts visible in the Azure DevOps UI.

---

## 10. Pull request checklist

Before opening a PR, confirm that all of these pass locally:

```bash
ruff check src/ tests/          # zero lint errors
mypy src/                        # zero type errors
pytest tests/                    # all tests pass, coverage ≥ 95 %
bandit -r src/ --skip B101       # no high/critical findings
```
