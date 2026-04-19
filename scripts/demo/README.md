# Demo — CDN edge logs on chDB + Iceberg

A self-contained demo that loads up to 10 million synthetic CDN-edge request rows into the `requests` Iceberg table and queries them through the read Lambda with chDB. The write Lambda **dual-writes** every batch into two parallel Iceberg backends so you can compare:

- **`glue`** (default for the demo) — regular S3 bucket + AWS Glue Data Catalog. Storage is in our own bucket so chDB's native `icebergS3()` table function works directly.
- **`s3tables`** — AWS S3 Tables managed Iceberg. Storage lives in AWS-owned `*--table-s3` buckets that refuse plain `GetBucketLocation`, which blocks chDB's S3 client; we fall back to a pyiceberg-materialize path here.

Built to be screenshot-friendly: every script prints with `rich` (panels, syntax-highlighted SQL, result tables, per-query timing line, run summary).

## What's here

| Script | What it does |
|---|---|
| `populate.py` | Generates N rows of realistic CDN logs in B parquet batches, drops them in the dropzone, and polls each batch through to the archive prefix so the progress bar reflects real ingestion. The write Lambda fans out to both backends. |
| `query_traffic.py` | Volume / geography / time / POP mix. Demonstrates `uniqExact`, top-K, hourly bucketing, window functions for share-of-total. |
| `query_performance.py` | Latency quantiles / slow paths / cache lift / response-size histogram. Demonstrates `quantile(...)`, `multiIf`-labelled boolean grouping, conditional averages. |
| `query_anomalies.py` | 5xx trends / offender IPs / p99 outliers / bot regex. Demonstrates `countIf`, WITH-clause for thresholds, `match()` regex, multi-class segmentation. |
| `_helpers.py` | Shared. Auto-discovers Lambda function name + data bucket from `terraform -chdir=terraform/main output -json`. Renders results with `rich`. |

## Auto-discovery

You don't need to pass `--function-name` or `--data-bucket` — the helpers read the live values out of the main Terraform state. Override order: env var → `terraform output` → hardcoded fallback.

| Default | Env var override | Terraform output key |
|---|---|---|
| Lambda function name | `CHDB_READ_FUNCTION` | `read_lambda_function_name` |
| Data bucket (dropzone) | `CHDB_DATA_BUCKET` | `data_bucket_name` |
| Asset name | `CHDB_ASSET` | — (defaults to `requests`) |
| AWS region | `AWS_REGION` | — (defaults to `us-east-1`) |

So as long as `terraform apply` has been run on `terraform/main`, every script Just Works.

## Backends and engines

The read Lambda has two orthogonal axes — pick a backend (where the Iceberg metadata + data files live) and an engine (how chDB gets at the data). The demo defaults to **`backend=glue, engine=iceberg_s3`** because that path scales the best — chDB streams parquet directly from S3, no Lambda-side materialization, no `/tmp`.

### Backends

| Backend | Storage | Catalog | Notes |
|---|---|---|---|
| **`glue`** (demo default) | Regular S3 bucket (`chdb-aws-prod-iceberg`) we own | AWS Glue Data Catalog | Plain S3 — `GetBucketLocation` / `ListObjectsV2` work, so chDB's native `icebergS3()` reads it without help. |
| `s3tables` | AWS-managed `*--table-s3` underlying buckets | S3 Tables Iceberg REST endpoint (SigV4) | Bucket policy refuses `GetBucketLocation`, so `icebergS3` can't be pointed at this directly. The Lambda falls back to pyiceberg-materialize. |

The write Lambda always writes to **both**. The Glue table is created lazily on the first append using the s3tables table's schema as the source of truth (`src/chdb_aws/write/iceberg_writer.py:_ensure_glue_table`).

### Engines

| Engine | What it does | Caching | Best for |
|---|---|---|---|
| **`iceberg_s3`** (demo default) | chDB's native `icebergS3('<table-root>')` reads the Iceberg metadata + data files directly from S3 | None at the Lambda layer; chDB does in-process row-group caching within a single query | Streaming reads of arbitrarily large tables. Per-call cost is roughly proportional to the columns + row groups touched. **Requires `backend=glue`** — `iceberg_s3 + s3tables` raises a clear error because of the bucket-policy block above. |
| `materialize` | `pyiceberg.scan().to_arrow()` once per snapshot → `/tmp` parquet → chDB `file()` | `/tmp` cache keyed on `(backend, namespace, asset, metadata_location)`; reused across invocations of the same warm container | Repeated diverse queries against an unchanging snapshot. Cold first query pays the full materialize cost (~5–20 s on 10M rows); subsequent queries are sub-second. The Lambda has a 4 GB `/tmp` so this comfortably handles ~25M rows of the demo schema. |
| `scan` | `pyiceberg.scan(selected_fields=…, row_filter=…)` pushes column projection + manifest-level predicate pruning before any parquet is written; per-call temp file | None | Narrow ad-hoc queries against a wide table where you want to materialize only the columns + rows the query touches. Note: pyiceberg pushdown changes what data chDB sees, so a `WHERE` you push down here is *additional* to whatever's in the SQL. |

Examples:

```sh
# default — glue + iceberg_s3 (chDB streams directly from S3)
uv run scripts/query.py --function-name chdb-aws-prod-read \
    --asset requests --sql 'SELECT count() FROM ${asset}'

# materialize from glue — full snapshot to /tmp, chDB queries the local file
uv run scripts/query.py --function-name chdb-aws-prod-read \
    --asset requests --sql 'SELECT count() FROM ${asset}' \
    --backend glue --engine materialize

# pushdown — only project status_code, push the WHERE down to pyiceberg
uv run scripts/query.py --function-name chdb-aws-prod-read \
    --asset requests --sql 'SELECT count() FROM ${asset}' \
    --backend s3tables --engine scan \
    --column status_code --where 'status_code >= 500'

# the same iceberg_s3 query but pointing at the S3 Tables backend — fails
# fast with an explanatory error (kept here so the day chDB gains an S3
# Tables-aware S3 client this works without further code changes)
uv run scripts/query.py --function-name chdb-aws-prod-read \
    --asset requests --sql 'SELECT count() FROM ${asset}' \
    --backend s3tables --engine iceberg_s3
```

The lambda payload shape is:

```json
{
  "asset": "requests",
  "sql":   "SELECT count() FROM ${asset}",
  "backend": "glue",
  "engine":  "iceberg_s3",
  "columns": ["status_code"],
  "where":   "status_code >= 500"
}
```

`backend` defaults to `s3tables` and `engine` to `materialize` at the Lambda layer, but the **demo scripts override both** to `backend=glue, engine=iceberg_s3`. Both `columns` and `where` are only meaningful when `engine == "scan"`.

## One-time prerequisites

Bootstrap the tfstate bucket, deploy the main stack, and push the Lambda image. You only do this once per account+region.

```sh
# 1. tfstate backend
cd terraform/bootstrap
terraform init && terraform apply -auto-approve

# 2. main stack — phase 1 (creates ECR + tables + Glue DB + iceberg bucket; no lambda yet)
cd ../main
terraform init && terraform apply -auto-approve

# 3. build + push the Lambda container image
cd ../..
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
./scripts/build-image.sh
AWS_REGION=us-east-1 AWS_ACCOUNT_ID=$AWS_ACCOUNT_ID REPO_NAME=chdb-aws-prod TAG=latest \
    ./scripts/push-image.sh

# 4. main stack — phase 2 (adds lambdas + S3 notification)
cd terraform/main
terraform apply -auto-approve \
    -var "image_uri=$AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/chdb-aws-prod:latest"
cd ../..
```

After the second apply the outputs will show:

```
data_bucket_name           = "chdb-aws-prod-data"
read_lambda_function_name  = "chdb-aws-prod-read"
write_lambda_function_name = "chdb-aws-prod-write"
glue_database_name         = "chdb_aws_prod"
iceberg_bucket_name        = "chdb-aws-prod-iceberg"
table_arns                 = { "requests" = "arn:aws:s3tables:..." }
glue_table_names           = { "requests" = "chdb_aws_prod.requests" }
```

## Run the demo

Each command is independent and uses auto-discovered defaults.

### populate

```sh
uv run scripts/demo/populate.py --rows 10000000 --batches 10 --archive-timeout 600
```

Expect ~150 s for 10M rows (≈ 65k rows/s end-to-end, including S3 upload + dual Iceberg write through the Lambda). Output ends with a summary panel showing rows, MB uploaded, wall time, rows/s, and MB/s.

Useful overrides:
- `--rows 1000000 --batches 4` for a quick smoke (~30 s)
- `--rows 25000000 --batches 25` to stress test
- `--no-wait` to skip the dropzone→archive polling (returns as soon as upload completes)

### query_traffic

```sh
uv run scripts/demo/query_traffic.py
```

4 queries: snapshot (count + uniqExact), top countries with share-of-total via window function, hourly volume buckets, top edge POPs with cache-hit rate.

### query_performance

```sh
uv run scripts/demo/query_performance.py
```

4 queries: overall latency quantile family (p50/p90/p95/p99/p999/max), slowest paths by p95, cache HIT vs MISS lift, response-size distribution by HTTP method.

### query_anomalies

```sh
uv run scripts/demo/query_anomalies.py
```

4 queries: 15-minute error-rate buckets, top offender IPs by 4xx/5xx, p99 outliers via WITH-clause threshold, bot vs client-lib vs browser segmentation by user-agent regex.

## Cold vs warm

Three independent things have to be "warm" for a query to feel snappy. The new default `iceberg_s3` engine and the original `materialize` engine warm up differently — table below summarizes both.

| Layer | What it is | Cost when cold |
|---|---|---|
| Lambda container | AWS spins up a new container, loads the image (~1.2 GB), runs Python init, opens the pyiceberg REST + Glue catalog connections. | ~3–8 s `INIT_REPORT` time. Goes cold after ~15 min idle or any AWS recycle. |
| `/tmp` parquet cache (`materialize` only) | Read Lambda materializes the entire current snapshot to a local parquet under `/tmp/chdb_cache_<hash>.parquet`. | ~10–18 s for 10M rows: pyiceberg fetches manifests + data files from S3, then `pq.write_table` lands them on `/tmp`. Cache key invalidates automatically when a new write advances the snapshot's `metadata_location`. |
| chDB process state (`iceberg_s3` only) | chDB caches manifest reads + S3 client state inside one Lambda invocation. | First query in a fresh container does S3 round trips for metadata + manifests + data files; ~5–15 s for 10M-row aggregates depending on column count touched. Subsequent queries against the same snapshot still hit S3 for data files but skip metadata. |

The cache is keyed on `sha1(backend + namespace + asset + metadata_location)` — see `src/chdb_aws/read/query.py:_cache_path`. New writes invalidate it automatically.

What this looks like in practice on the **10M-row** dataset, demo defaults (`glue + iceberg_s3`):

| Scenario | First query | Queries 2–4 | Notes |
|---|---|---|---|
| Right after `terraform apply` (cold container) | **~14 s** | ~1.4–2.9 s each | First query's wall time is mostly Lambda INIT + chDB process bootstrap. |
| Container kept warm, repeated runs | ~2–3 s | ~1–2 s each | Steady state — every query streams real S3 reads but skips the slow init. |

For comparison, **`glue + materialize`** on the same 10M-row dataset:

| Scenario | First query | Queries 2–4 |
|---|---|---|
| Cold container, cold cache | **~18 s** | sub-second each |
| Warm container, warm cache | ~4 s (cold chDB on the cached parquet) | sub-second each |

So the trade-off:

- `materialize` pays one big upfront cost per snapshot, then everything else is sub-second. Best when you'll run many queries against the same snapshot.
- `iceberg_s3` pays a smaller, more consistent cost per query (no big upfront tax). Best for one-shot ad-hoc queries or when the snapshot churns often.

The demo defaults to `iceberg_s3` because it scales: there's no `/tmp` ceiling and no per-snapshot cliff. If you'll run a tight loop of queries against the same snapshot, override with `--engine materialize` for the warm-cache wins.

## Why so fast?

For the `iceberg_s3` engine on `glue`, the read path is:

1. **`boto3.client("lambda").invoke(...)`** from your laptop → AWS Lambda API → warm container. ~50–150 ms of fixed AWS-side overhead.
2. **`pyiceberg.glue_catalog.load_table(...)`** — one HTTPS hop to AWS Glue to read the current `metadata_location`. ~50–100 ms.
3. **`chdb.query("... FROM icebergS3('s3://chdb-aws-prod-iceberg/requests/')")`** — chDB's native S3 client reads metadata.json → manifest list → manifests → data files. Vectorized SIMD execution over column batches with parquet row-group statistics for pruning. The chDB time you see is dominated by the actual data-file reads from S3 (a few hundred ms to a few seconds depending on how many columns the query touches and how much data flows back).
4. **Response payload** back through Lambda → boto3.

For the `materialize` engine, swap step 3 for a `file()` call against the locally-cached parquet on `/tmp` — that's a single-digit-millisecond mmap read once the cache is warm.

What is **not** happening on either path:

- No `S3:ListObjects` on the data bucket (Iceberg metadata enumerates files explicitly).
- No remote query engine — the SQL execution is happening inside the Lambda's Python process via chDB.
- For `iceberg_s3`: nothing materialized to `/tmp`. The Lambda can scale to whatever the table is, limited only by chDB's per-call memory.

## Tip — running for screenshots

To capture a clean run, warm everything once and discard the output, then capture the runs you want:

```sh
# warm the container + chDB state once (cold-path run, ~15 s on 10M)
uv run scripts/demo/query_traffic.py >/dev/null

# now capture
uv run scripts/demo/query_traffic.py
uv run scripts/demo/query_performance.py
uv run scripts/demo/query_anomalies.py
```

Reference total wall time on the **10M-row** dataset, 3008 MB / 4 GB-tmp Lambda, `us-east-1`. `iceberg_s3` numbers are the median of three back-to-back warm runs. `materialize` numbers are after a cache-warming run.

| Script | `iceberg_s3 + glue` (default) | `materialize + glue` (warm cache) | dominant cost in iceberg_s3 |
|---|---|---|---|
| `query_traffic.py` | **~12 s** (4 queries) | ~7 s | Q1 `count + uniqExact` over the full table (~6 s wall, ~5.7 s chDB) |
| `query_performance.py` | **~6 s** (4 queries) | ~3 s | even split — quantile aggregates over 4 different columns |
| `query_anomalies.py` | **~24 s** (4 queries) | ~5 s | Q2 top-10 offender IPs (~15 s) — group by `client_ip` is high-cardinality (~850k uniques) and the query does several conditional aggregates per group |

The anomalies run is dominated by one heavy query — strip it out (or cap with `LIMIT` upstream of the group-by) and the suite drops to ~9 s.

Cold-start additions on top of warm steady-state: ~3–8 s for Lambda container init + chDB process bootstrap, paid once per ~15-min idle period.

If you want to *show off the cold path* in a screenshot, push a new image or wait 15+ minutes between runs.

## Tearing down

`force_destroy = true` is set on the data bucket, the iceberg bucket, and the ECR repo, so destroy is one shot:

```sh
cd terraform/main
terraform destroy -auto-approve \
    -var "image_uri=$AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/chdb-aws-prod:latest"
```

The tfstate bucket from `terraform/bootstrap` persists by design — it holds the main stack's state. Destroy it manually (`aws s3 rb s3://chdb-aws-tfstate --force`) if you really mean to wipe everything.

## Screenshots

The current screenshots were captured against 1M rows on a warm container + warm cache. Re-shoot at 10M for a refreshed demo:

```sh
uv run scripts/demo/query_traffic.py     # → traffic.png
uv run scripts/demo/query_performance.py # → perf.png
uv run scripts/demo/query_anomalies.py   # → outliers.png
```

### Traffic Overview — `query_traffic.py`

Volume snapshot, top countries with share-of-total via window function, hourly buckets, top edge POPs with cache-hit rate.

![traffic](traffic.png)

### Performance Analysis — `query_performance.py`

Latency quantile family (p50/p90/p95/p99/p999/max), slowest paths by p95, cache HIT vs MISS lift, response-size distribution by HTTP method.

![performance](perf.png)

### Error & Anomaly Detection — `query_anomalies.py`

15-minute error-rate buckets, top offender IPs by 4xx/5xx, p99 outliers via WITH-clause threshold, bot/client-lib/browser segmentation by user-agent regex.

![anomalies](outliers.png)
