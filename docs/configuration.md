# Configuration Reference

Every pipeline is defined by a single YAML file placed under `configs/pipelines/`.
The file is loaded by `ConfigLoader`, which:

1. Replaces every `${VAR_NAME}` placeholder with the value of the environment
   variable `VAR_NAME`.  If the variable is not set the placeholder is kept
   verbatim so the validation error message clearly shows which variable is
   missing.
2. Validates the resulting dictionary against the Pydantic v2 model
   `PipelineConfig`.  A `ValidationError` is raised immediately if any required
   field is absent or a value has the wrong type/enumeration value.

---

## Top-level fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | `string` | ✅ | — | Human-readable pipeline identifier used in logs and summary output. |
| `version` | `string` | | `"1.0"` | Semantic version of the pipeline config; informational only. |
| `source` | `RedshiftConfig` | ✅ | — | Redshift data source. See [source](#source-redshiftconfig). |
| `transformation` | `TransformationConfig` | ✅ | — | Transformation engine settings. See [transformation](#transformation-transformationconfig). |
| `medallion` | `MedallionConfig` | ✅ | — | Bronze / Silver / Gold layer paths. See [medallion](#medallion-medallionconfig). |
| `storage` | `StorageConfig` | ✅ | — | Final output storage settings. See [storage](#storage-storageconfig). |

---

## `source` — `RedshiftConfig`

Connection details for the Amazon Redshift cluster that serves as the data
source.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `host` | `string` | ✅ | — | Redshift cluster endpoint hostname. |
| `port` | `integer` | | `5439` | Redshift port number. |
| `database` | `string` | ✅ | — | Database name to connect to. |
| `schema_name` | `string` | | `"public"` | Default schema; informational — queries in `tables` should be fully qualified. |
| `username` | `string` | ✅ | — | Database user name. Use `${ENV_VAR}` to avoid committing credentials. |
| `password` | `string` | ✅ | — | Database password. Use `${ENV_VAR}`. |
| `tables` | `list[RedshiftSourceTable]` | ✅ | — | One or more source table definitions. |

### `RedshiftSourceTable`

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | `string` | ✅ | — | Logical name used as the key in the DataFrames dict and in output paths. |
| `query` | `string` | ✅ | — | SQL `SELECT` statement executed against Redshift. |
| `schema_name` | `string` | | `"public"` | Informational; the query itself determines the schema. |

**Example:**

```yaml
source:
  host: "${REDSHIFT_HOST}"
  port: 5439
  database: "${REDSHIFT_DB}"
  username: "${REDSHIFT_USER}"
  password: "${REDSHIFT_PASSWORD}"
  tables:
    - name: orders
      query: "SELECT order_id, customer_id, amount FROM public.orders"
    - name: order_items
      query: "SELECT item_id, order_id, product_id FROM public.order_items"
```

---

## `transformation` — `TransformationConfig`

Selects whether AWS Glue or AWS EMR is used for the transformation step.
Exactly one of `glue` or `emr` must be present, matching the value of
`engine`.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `engine` | `"glue"` \| `"emr"` | ✅ | — | Transformation engine to use. |
| `glue` | `GlueConfig` | When `engine: glue` | — | Glue job settings. |
| `emr` | `EMRConfig` | When `engine: emr` | — | EMR step settings. |

### `GlueConfig`

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `job_name` | `string` | ✅ | — | Name of the AWS Glue job in the Data Catalog. |
| `region` | `string` | | `"us-east-1"` | AWS region of the Glue job. |
| `iam_role` | `string` | ✅ | — | ARN of the IAM role assumed by the Glue job. |
| `script_location` | `string` | ✅ | — | S3 URI of the PySpark/Glue ETL script. |
| `temp_dir` | `string` | ✅ | — | S3 URI used by Glue for spill and temporary files. |
| `arguments` | `dict[str, str]` | | `{}` | Extra `--key value` arguments forwarded to every job run. |
| `redshift_connection_name` | `string` \| `null` | | `null` | Name of an existing Glue Data Catalog connection of type `JDBC` pointing at Redshift.  When set, it is automatically injected into every job run as the `--redshift_connection_name` argument so the Glue script can reference it without hard-coding credentials.  Create the connection with `GlueProcessor.create_redshift_connection()`. |
| `subnet_id` | `string` \| `null` | | `null` | VPC subnet ID for `PhysicalConnectionRequirements` when *creating* a new Glue connection. |
| `security_group_ids` | `list[str]` | | `[]` | VPC security-group IDs for the Glue connection's physical requirements. |
| `availability_zone` | `string` \| `null` | | `null` | Availability zone for the Glue connection's physical requirements. |

**Example (Glue engine with Redshift connection):**

```yaml
transformation:
  engine: glue
  glue:
    job_name: orders-etl-job
    region: us-east-1
    iam_role: "${GLUE_IAM_ROLE}"
    script_location: "s3://${S3_BUCKET}/scripts/orders_transform.py"
    temp_dir: "s3://${S3_BUCKET}/temp/"
    redshift_connection_name: "${GLUE_REDSHIFT_CONNECTION_NAME}"
    subnet_id: "${GLUE_SUBNET_ID}"
    security_group_ids:
      - "${GLUE_SECURITY_GROUP_ID}"
    availability_zone: "${GLUE_AZ}"
    arguments:
      "--enable-metrics": "true"
```

### `EMRConfig`

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `cluster_id` | `string` | ✅ | — | EMR cluster ID (e.g. `j-ABCDEFG123456`). |
| `region` | `string` | | `"us-east-1"` | AWS region of the EMR cluster. |
| `spark_script` | `string` | ✅ | — | S3 URI of the `spark-submit` script. |
| `arguments` | `list[str]` | | `[]` | Additional positional arguments passed after the script path to `spark-submit`. |

**Example (EMR engine):**

```yaml
transformation:
  engine: emr
  emr:
    cluster_id: "${EMR_CLUSTER_ID}"
    region: us-east-1
    spark_script: "s3://${S3_BUCKET}/scripts/customers_transform.py"
    arguments:
      - "--input-path"
      - "s3://${S3_BUCKET}/bronze/customers"
      - "--output-path"
      - "s3://${S3_BUCKET}/silver/customers"
```

---

## `medallion` — `MedallionConfig`

Paths and output formats for each of the three Medallion layers.

| Field | Type | Required | Description |
|---|---|---|---|
| `bronze` | `MedallionLayerConfig` | ✅ | Raw ingestion layer. |
| `silver` | `MedallionLayerConfig` | ✅ | Cleaned and lightly transformed layer. |
| `gold` | `MedallionLayerConfig` | ✅ | Business-ready aggregation layer. |

### `MedallionLayerConfig`

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `path` | `string` | ✅ | — | Base path (S3 URI or local) for this layer. Each table is written to `{path}/{table_name}/`. |
| `format` | `"parquet"` \| `"csv"` \| `"delta"` \| `"iceberg"` \| `"hudi"` | | `"parquet"` | Output format for this layer. Delta, Iceberg, and Hudi require a Spark runtime (Glue/EMR job). |
| `options` | `dict[str, str]` | | `{}` | Format-specific options forwarded to the writer (e.g. `compression: snappy`). |

**Example:**

```yaml
medallion:
  bronze:
    path: "s3://${S3_BUCKET}/bronze/orders"
    format: parquet
  silver:
    path: "s3://${S3_BUCKET}/silver/orders"
    format: parquet
  gold:
    path: "s3://${S3_BUCKET}/gold/orders"
    format: parquet
```

---

## `storage` — `StorageConfig`

Final output storage for the pipeline's Gold-layer results.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `format` | `"parquet"` \| `"csv"` \| `"delta"` \| `"iceberg"` \| `"hudi"` | | `"parquet"` | Output format. |
| `path` | `string` | ✅ | — | Destination path (S3 URI or local filesystem). |
| `options` | `dict[str, str]` | | `{}` | Format-specific options (e.g. `compression: gzip`). |

**Example:**

```yaml
storage:
  format: parquet
  path: "s3://${S3_BUCKET}/output/orders"
  options:
    compression: snappy
```

---

## Environment variable interpolation

All `string` values in the YAML file are scanned for `${VAR_NAME}` patterns
before validation.  The replacement rules are:

- If `VAR_NAME` is set in the environment, its value replaces the placeholder.
- If `VAR_NAME` is **not** set, the original placeholder `${VAR_NAME}` is kept.
  This means validation will still succeed but the field value will contain the
  literal string `"${VAR_NAME}"` — which will cause an obvious failure at
  runtime (e.g. Redshift connection refused), making it easy to diagnose.

Interpolation is recursive — it applies to strings nested at any depth inside
dicts and lists.
