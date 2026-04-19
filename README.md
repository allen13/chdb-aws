# chdb-aws

Serverless analytics on S3 Tables (Iceberg) using [chDB](https://github.com/chdb-io/chdb) — the ClickHouse engine, embedded in a Python Lambda. Drop parquet files into an S3 dropzone, a write Lambda appends them to an Iceberg table, and a read Lambda answers ad-hoc SQL with chDB.

- **Storage**: AWS S3 Tables (managed Iceberg) — compaction and snapshot maintenance handled by AWS.
- **Write path**: S3 ObjectCreated → Lambda → `pyiceberg` REST catalog → Iceberg table.
- **Read path**: Lambda loads the current snapshot with `pyiceberg`, materializes it once to `/tmp` (cached across warm invocations keyed on the metadata location), and serves SQL via chDB.
- **Schema lifecycle**: declared once in Terraform (`terraform/main/terraform.tfvars`), applied via the `chdb_aws` module. A `generate-schema` skill under `.claude/skills/` writes new assets for you with the right Iceberg types and evolution guarantees.

## Repo layout

```
src/chdb_aws/         # Lambda code (read/ + write/)
terraform/modules/    # reusable chdb_aws module (ECR, Lambda, S3, IAM, S3 Tables)
terraform/main/       # prod stack
terratest/            # end-to-end Go test (InitAndApply → upload → query → Destroy)
scripts/              # build-image.sh, push-image.sh, generate_test_data.py, query.py
scripts/demo/         # the CDN-logs demo (see below)
.claude/skills/       # generate-schema skill for authoring new assets
```

## Deploy

```sh
cd terraform/main
terraform init
terraform apply                       # creates ECR, data bucket, table bucket, namespace, tables
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REPO_NAME=chdb-aws-prod ../../scripts/build-image.sh
AWS_REGION=us-east-1 AWS_ACCOUNT_ID=$AWS_ACCOUNT_ID REPO_NAME=chdb-aws-prod \
    ../../scripts/push-image.sh
terraform apply -var "image_uri=$AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/chdb-aws-prod:latest"
```

## Demo — CDN edge request logs

`terraform/main/terraform.tfvars` ships with a single asset `requests` that models one week of CDN-edge access logs (13 columns: `ts`, `edge_pop`, `client_ip`, `country`, `http_method`, `path`, `status_code`, `bytes_sent`, `response_time_ms`, `cache_hit`, `user_agent`, `referrer`, `request_id`). It's a realistic shape that exercises most of chDB's query surface.

### 1. Populate

```sh
uv run scripts/demo/populate.py --rows 1000000 --batches 10
```

Generates one million rows with realistic skew (lognormal latencies, 90% cache-hit rate, 200s dominating status codes, long-tail client IPs, ~30 edge POPs, ~25 countries), writes them as 10 parquet files, drops each in `s3://<bucket>/assets/requests/dropzone/`, and polls for the archive marker so the progress bar reflects actual ingestion.

### 2. Query — three analytical workloads

Each script runs 4 queries against the read Lambda and prints each result as a rich table plus a per-query metrics line (chDB wall time, rows read, bytes read).

```sh
uv run scripts/demo/query_traffic.py        # volume / geography / time / POP mix
uv run scripts/demo/query_performance.py    # latency quantiles / slow paths / cache lift
uv run scripts/demo/query_anomalies.py      # 5xx trends / offender IPs / outliers / bots
```

The first query on a cold Lambda pays a ~5–10 s scan of the Iceberg table into `/tmp`. Every subsequent query on a warm Lambda (same snapshot) is sub-second — the read path caches the materialized parquet keyed on the table's current metadata location, so new writes naturally invalidate it.

#### Traffic overview

![traffic screenshot](scripts/demo/traffic.png)

Demonstrates `uniqExact`, top-K with share-of-total via window functions, `toStartOfHour` bucketing, cache-hit aggregates.

#### Performance analysis

![performance screenshot](scripts/demo/perf.png)

Demonstrates `quantile(...)`, `multiIf` for labelled boolean grouping, quantile families grouped by a dimension, response-size distributions.

#### Error & anomaly detection

![anomalies screenshot](scripts/demo/outliers.png)

Demonstrates `countIf` conditional aggregates, WITH-clause for p99 thresholds, regex matching (`match(user_agent, '(?i)bot|crawler|…')`), multi-class segmentation with `multiIf`.

## Testing

```sh
# Unit tests (mocked S3 + s3tables)
uv run pytest

# Full lifecycle test against a dev stack
REPO_NAME=chdb-aws-testbed ./scripts/build-image.sh
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text) \
AWS_REGION=us-east-1 REPO_NAME=chdb-aws-testbed ./scripts/push-image.sh
cd terratest
CHDB_AWS_IMAGE_URI=$AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/chdb-aws-testbed:latest \
AWS_REGION=us-east-1 go test -v -run TestChdbAwsDev -timeout 20m
```

## Schema evolution

Use the `generate-schema` skill in Claude Code:

> "Add a new asset `api_audit` with columns …"

It edits `terraform/main/terraform.tfvars` in place, respecting Iceberg type rules, PyArrow → Iceberg mapping (`pa.int64` → `long`, `pa.timestamp('us', tz='UTC')` → `timestamptz`, etc.), and safe-evolution rules (no narrowing, no type-family changes). See `.claude/skills/generate-schema/SKILL.md` for the full reference.
