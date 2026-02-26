# Glue Redshift Connection Guide

This guide explains how to use a named **AWS Glue Data Catalog connection** of
type `JDBC` to connect Glue jobs to Amazon Redshift, instead of embedding raw
JDBC credentials directly in job arguments.

---

## Why use a Glue connection?

| Direct JDBC args | Glue Data Catalog connection |
|---|---|
| Credentials visible in job run history | Credentials stored once in the Data Catalog; never appear in job runs |
| VPC/networking config repeated per job | VPC config stored in the connection; reused by all jobs that reference it |
| Rotating credentials requires updating every pipeline config | Update the connection once; all jobs pick it up automatically |
| No AWS-managed SSL enforcement | `JDBC_ENFORCE_SSL` flag stored centrally |

---

## How it works

```
Pipeline YAML
  └── transformation.glue.redshift_connection_name: "my-redshift-conn"
         │
         ▼
  GlueProcessor.run_job()
  injects: --redshift_connection_name my-redshift-conn
         │
         ▼
  Glue ETL Script (PySpark)
  reads:  args["redshift_connection_name"]
  calls:  glueContext.create_dynamic_frame_from_options(
              connection_type="redshift",
              connection_options={
                  "url": "jdbc:redshift://...",
                  "dbtable": "public.orders",
                  "redshiftTmpDir": args["TempDir"],
              },
          )
```

The `GlueProcessor` never reads credentials from the Glue connection itself —
it only passes the **name** of the connection to the job.  The Glue job runtime
resolves the credentials when it establishes the JDBC connection.

---

## Prerequisites

1. An Amazon Redshift cluster accessible from the Glue job's VPC.
2. A VPC subnet and security group that allow outbound TCP on port `5439`
   (or your custom Redshift port) from the Glue job's ENI.
3. An IAM role for the Glue job with at least:
   - `glue:GetConnection`
   - `ec2:DescribeSubnets`, `ec2:DescribeSecurityGroups`,
     `ec2:DescribeVpcEndpoints` (for VPC connectivity)
   - `redshift:GetClusterCredentials` (if using IAM auth)

---

## Step 1 — Set connection fields in `GlueConfig`

In your pipeline YAML, add the four new fields under `transformation.glue`:

```yaml
transformation:
  engine: glue
  glue:
    job_name: orders-etl-job
    region: us-east-1
    iam_role: "${GLUE_IAM_ROLE}"
    script_location: "s3://${S3_BUCKET}/scripts/orders_transform.py"
    temp_dir: "s3://${S3_BUCKET}/temp/"

    # ── Glue Redshift connection ──────────────────────────────────────
    redshift_connection_name: "${GLUE_REDSHIFT_CONNECTION_NAME}"
    subnet_id: "${GLUE_SUBNET_ID}"
    security_group_ids:
      - "${GLUE_SECURITY_GROUP_ID}"
    availability_zone: "${GLUE_AZ}"
    # ─────────────────────────────────────────────────────────────────

    arguments:
      "--enable-metrics": "true"
```

Set the environment variables:

```bash
export GLUE_REDSHIFT_CONNECTION_NAME=my-redshift-conn
export GLUE_SUBNET_ID=subnet-0abc1234def567890
export GLUE_SECURITY_GROUP_ID=sg-0abc1234def567890
export GLUE_AZ=us-east-1a
```

---

## Step 2 — Create the connection (one-time)

Run this **once** per environment.  The connection is stored in the AWS Glue
Data Catalog and persists across job runs.

```python
from src.config.loader import ConfigLoader
from src.processors.glue import GlueProcessor

config = ConfigLoader().load("configs/pipelines/example_orders_pipeline.yaml")

processor = GlueProcessor(config.transformation.glue)
connection_name = processor.create_redshift_connection(
    config.source,
    enforce_ssl=True,   # recommended for production
)
print(f"Created connection: {connection_name}")
```

### What `create_redshift_connection` does

1. Builds a `jdbc:redshift://{host}:{port}/{database}` JDBC URL.
2. Calls `glue:CreateConnection` with:
   - `ConnectionType: JDBC`
   - `ConnectionProperties`: JDBC URL, USERNAME, PASSWORD, JDBC_ENFORCE_SSL
   - `PhysicalConnectionRequirements`: SubnetId, SecurityGroupIdList,
     AvailabilityZone (if the corresponding config fields are set)
3. Returns the connection name on success.

> **Note:** If the connection already exists the API call raises
> `AlreadyExistsException`.  You can use `get_redshift_connection()` to check
> first, or delete and recreate it when rotating credentials.

---

## Step 3 — Associate the connection with the Glue job

Connections are linked to a Glue job at **job creation time**, not at run time.
When you create (or update) the job in the AWS console or via IaC (CloudFormation
/ Terraform / CDK), add the connection under **Connections**:

**AWS Console:** Glue → Jobs → *your-job* → Edit → Connections → Add

**Terraform example:**

```hcl
resource "aws_glue_job" "orders_etl" {
  name     = "orders-etl-job"
  role_arn = var.glue_iam_role

  command {
    script_location = "s3://${var.s3_bucket}/scripts/orders_transform.py"
  }

  connections = ["my-redshift-conn"]   # ← the connection name

  default_arguments = {
    "--TempDir"          = "s3://${var.s3_bucket}/temp/"
    "--enable-metrics"   = "true"
  }
}
```

---

## Step 4 — Run the pipeline

```python
from src.config.loader import ConfigLoader
from src.pipeline import Pipeline

config = ConfigLoader().load("configs/pipelines/example_orders_pipeline.yaml")
summary = Pipeline(config).run()
```

`GlueProcessor.run_job()` automatically injects
`--redshift_connection_name my-redshift-conn` into the job arguments so your
PySpark script receives it:

```python
# orders_transform.py  (Glue ETL script)
import sys
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "TempDir", "redshift_connection_name"],
)
sc = SparkContext()
glueContext = GlueContext(sc)

# Retrieve JDBC properties from the Glue Data Catalog connection so that the
# script never contains hard-coded credentials or host names.
glue_client = boto3.client("glue")
conn_props = glue_client.get_connection(
    Name=args["redshift_connection_name"]
)["Connection"]["ConnectionProperties"]

orders_df = glueContext.create_dynamic_frame_from_options(
    connection_type="redshift",
    connection_options={
        "url": conn_props["JDBC_CONNECTION_URL"],
        "user": conn_props["USERNAME"],
        "password": conn_props["PASSWORD"],
        "dbtable": "public.orders",
        "redshiftTmpDir": args["TempDir"],
    },
    transformation_ctx="orders_source",
)
```

---

## Verifying the connection

```python
from src.processors.glue import GlueProcessor
from src.config.schema import GlueConfig

processor = GlueProcessor(config.transformation.glue)
details = processor.get_redshift_connection("my-redshift-conn")
print(details["ConnectionType"])          # "JDBC"
print(details["ConnectionProperties"])   # URL, USERNAME, etc.
```

You can also test the connection from the AWS Glue console:
**Glue → Connections → *my-redshift-conn* → Test connection**.

---

## Rotating credentials

When you rotate the Redshift password:

1. Update `${REDSHIFT_PASSWORD}` in your secrets manager / environment.
2. Delete the old connection:

   ```bash
   aws glue delete-connection --connection-name my-redshift-conn
   ```

3. Re-run `create_redshift_connection()` (Step 2 above).

No pipeline YAML changes are required.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `AlreadyExistsException` on `create_connection` | Connection was created before | Delete it first or use `get_redshift_connection` to verify |
| Glue job fails with `Connection refused` | Security group or subnet misconfigured | Check that the SG allows egress TCP `5439` to the Redshift SG |
| `Connection not found` in Glue job logs | Job not associated with the connection | Add the connection in the job definition (Step 3) |
| `ValidationError: redshift_connection_name must be set` | `create_redshift_connection()` called without the field | Set `redshift_connection_name` in the YAML / `GlueConfig` |
