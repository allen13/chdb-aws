# Architectural Review: chDB on Lambda + S3 Tables

*A review of this repo's approach to serverless OLAP, the trade-offs it makes, and the competitive landscape it sits in — with a direct argument for why we'd build this instead of buying Snowflake.*

---

## 1. Thesis

Interactive analytics does not require a warehouse. It requires three things:

1. A columnar file format on cheap object storage.
2. A table-level metadata layer that gives you snapshots, schema evolution, and safe concurrent writes.
3. An engine that can read those files and run SQL.

This repo binds one credible answer to each: **Parquet on S3**, **Apache Iceberg** (via AWS S3 Tables *and* Glue, in parallel), and **chDB** — ClickHouse's engine embedded in a Python Lambda. The result is a system where storage is a commodity line item, compute is pay-per-query, and the data stays in an open format that any Iceberg reader — Spark, Trino, DuckDB, Flink, Snowflake itself — can read on day one and still read a decade from now.

**Who this stack is for:** teams with moderate-scale analytics (GB to low-TB per query), irregular or bursty query volume, and a strong preference for owning their data in open formats rather than renting a warehouse. It is not a Snowflake replacement for every workload — Section 6 is honest about where Snowflake still wins.

---

## 2. The Architecture in This Repo

### 2.1 Data flow

```
                    ┌─────────────────────────────┐
  producer ───►     │ s3://<data>/assets/<asset>/ │
   (parquet)        │           dropzone/         │
                    └──────────────┬──────────────┘
                                   │  S3 ObjectCreated
                                   ▼
                        ┌────────────────────┐
                        │   write Lambda     │   1 GB mem, 300s timeout
                        │ chdb_aws.write.*   │   container image
                        └─────────┬──────────┘
                                  │  pyiceberg append (atomic dual-write)
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
        ┌───────────────────────┐   ┌──────────────────────────┐
        │ S3 Tables (REST cat.) │   │ Glue catalog + S3 bucket │
        │ managed Iceberg       │   │ "regular" Iceberg        │
        └───────────┬───────────┘   └────────────┬─────────────┘
                    └────────────┬───────────────┘
                                 │
                                 ▼
                        ┌────────────────────┐
                        │    read Lambda     │   3 GB mem, 4 GB /tmp, 300s
                        │  chdb_aws.read.*   │   container image
                        └─────────┬──────────┘
                                  │  engine = materialize | scan | iceberg_s3
                                  ▼
                         chDB (embedded CH) ──► JSONCompact response
```

Ingested parquets move from `dropzone/` to `archive/` after successful commit and are tagged for 30-day expiration.

### 2.2 Runtime (from `terraform/modules/chdb_aws/lambda.tf`)

| Function | Memory | `/tmp` | Timeout | Packaging |
|----------|-------:|-------:|--------:|-----------|
| write    | 1024 MB | 512 MB (default) | 300 s | container (ECR, Python 3.13 base) |
| read     | 3008 MB | **4096 MB** | 300 s | container (ECR, Python 3.13 base) |

The read Lambda gets oversized ephemeral storage because the default `materialize` engine scans the current Iceberg snapshot into `/tmp` once per metadata version and then reuses the cached parquet for every warm invocation against that version. `lambda.tf:42-47` explains the sizing rationale.

### 2.3 Three read engines, two backends

From `src/chdb_aws/read/query.py:1-33`:

| Axis | Options | Notes |
|------|---------|-------|
| **Backend** | `s3tables` (default) \| `glue` | Where the Iceberg metadata lives. Both receive every write. |
| **Engine**  | `materialize` (default) \| `scan` \| `iceberg_s3` | How the Lambda feeds data into chDB. |

**`materialize`** — pyiceberg reads the whole snapshot, writes it to `/tmp/chdb_cache_<sha>.parquet`, and lets chDB query the file. Cache key is `(backend, namespace, asset, metadata_location)`, so warm invocations against the same snapshot are sub-second. A new write rolls the metadata pointer and naturally invalidates the cache (`query.py:127-154`).

**`scan`** — pyiceberg `Table.scan(selected_fields=…, row_filter=…)` pushes column projection and manifest-level predicate pruning down *before* any parquet write to `/tmp`. No cache, per-call temp file. Right choice when the query touches a small slice of a wide table (`query.py:162-189`).

**`iceberg_s3`** — chDB's native `icebergS3()` table function reads the table root directly from S3. **Only works with `backend=glue`.** The underlying S3 Tables bucket (`*--table-s3`) has a service-managed policy that denies `GetBucketLocation`, which ClickHouse calls before any read. The Lambda surfaces a clear error rather than letting chDB report the cryptic 405 (`query.py:197-220`).

### 2.4 Why two backends

S3 Tables is the future — managed compaction, managed snapshots, a catalog you don't operate. But today the bucket-policy gotcha above means chDB's best-performing read path cannot touch it. Rather than wait for AWS to relax that policy, or settle for the `materialize` penalty on every query, the write Lambda commits every append to **both** catalogs atomically (`src/chdb_aws/write/iceberg_writer.py:33-82`). The parallel Glue backend exists solely so `engine=iceberg_s3` has somewhere to read from. The day S3 Tables allows `GetBucketLocation`, the Glue mirror becomes deletable.

This is the kind of trade-off that disappears into a warehouse — and one we chose to make explicit here rather than hide.

### 2.5 Notable design choices

- **Schema declared in Terraform** (`terraform/main/terraform.tfvars`). Iceberg types (`timestamptz`, required flags) survive round-trips; had we gone Glue-first, Hive's type system would have lossy-coerced them.
- **Snapshot-keyed `/tmp` cache.** Simple, correct, and free. The metadata location is the snapshot identity — when it changes, a new cache file is written and the old one is eventually reclaimed by Lambda's container recycling.
- **Container images, not zip packages.** chDB's wheel and the pyiceberg+pyarrow+boto3 dependency graph are well over Lambda's 250 MB zip limit. The Dockerfile uses `uv` for reproducible lockfile-driven builds.
- **No Glue Data Catalog integration with S3 Tables.** The two catalogs are independent and mirrored in application code. Keeping them decoupled means a failure in one does not poison the other — and schema is sourced from Terraform, not from either catalog.

---

## 3. What Works Well

- **No cluster to size, pause, or scale.** There is no warehouse. Cold queries hit a cold Lambda; warm queries hit a warm one. Autoscaling is whatever the Lambda service decides.
- **Data is ours, in the clear.** The parquet files are standard Parquet. The metadata is standard Iceberg. Any engine that speaks Iceberg — Spark, Trino, Flink, DuckDB, Athena, Snowflake's external Iceberg support — can query the same bytes without an export step.
- **Cost profile that matches workload.** At zero queries, you pay S3 storage and nothing else. At one query, you pay a few Lambda GB-seconds and a handful of S3 requests. There is no per-second warehouse minimum.
- **Schema evolution is safe and boring.** Add a column: Iceberg writes a new schema version; existing parquet is read with null-fill for the missing field. Drop or rename: tracked by field ID. No rewriting files.
- **No partitioning strategy to defend.** Iceberg's hidden partitioning plus chDB's columnar pruning handle the common cases. You can add partition specs later if a specific workload demands it, without rewriting history.
- **The whole thing fits in one module.** `terraform/modules/chdb_aws/` + `src/chdb_aws/` is small enough to read in an afternoon.

---

## 4. Where It Breaks (Honest Limits)

This stack is not magic. It makes specific trade-offs, and some of them bite.

- **Single-Lambda concurrency.** chDB runs in-process inside one Lambda invocation. True parallelism requires fanning out across invocations (Step Functions, SQS, or application code), which in turn requires the query to be decomposable. For a single expensive scan, one Lambda is a single core.
- **Ephemeral `/tmp` ceiling.** Lambda caps `/tmp` at 10 GB. The default 4 GB here covers ~25M rows of the demo schema comfortably; past that you either bump the ceiling, switch to `engine=scan` (smaller slice), or move to `engine=iceberg_s3` (no local materialization at all).
- **Cold starts on container images.** 2–5 seconds for the first invocation of a fresh execution environment. Subsequent warm invocations are sub-second, but p99 dashboards that can't tolerate an occasional cold start need provisioned concurrency — which re-introduces a cost floor.
- **No query federation.** Each Lambda invocation is a silo. You can't join across two tables that live in different assets without materializing one of them client-side, or broadening the Lambda's scope.
- **No governance or query history baked in.** Snowflake and Databricks give you query history, cost attribution, access grants, row/column security, and data lineage in the box. Here you get CloudWatch logs, CloudTrail, and whatever you build on top.
- **Writes are not cross-file transactional.** Each dropped parquet is its own Iceberg commit. If an upstream system produces N related files, a consumer can observe the partial state between commits. Group them in a single parquet upstream, or add an application-level transaction boundary.
- **No materialized views, no incremental state.** chDB is stateless per invocation. Every query is a fresh scan (plus whatever the `/tmp` cache buys you). ClickHouse Cloud's materialized views, projections, and merge-tree optimizations do not apply here.

---

## 5. Competing Alternatives

The serverless-OLAP-over-open-formats space has real competition. A summary first, then each contender in more detail.

### 5.1 Summary matrix

| Product | Data ownership | Ops burden | Cost when idle | Cost at scale | Concurrency | Cold start | Iceberg native |
|---|---|---|---|---|---|---|---|
| **This stack (chDB + S3 Tables + Lambda)** | Full — open Iceberg | Low — no cluster | ~$0 (S3 only) | Linear in query volume | Per-Lambda | 2–5 s | Yes (+ dual Glue) |
| **Snowflake** | Partial — Iceberg Tables narrow the gap | Very low — fully managed | Per-second warehouse minimum | High at sustained volume | Excellent | <1 s (warehouse on) | Yes (Iceberg Tables) |
| **Databricks SQL (Photon)** | Full with UniForm / Delta | Low–medium | SQL warehouse minimums | Moderate–high | Excellent | Sub-second (serverless SQL) | Via UniForm |
| **Amazon Athena** | Full — reads your S3 | None | $0 | $5/TB scanned | Good (per-query) | <2 s | Yes |
| **Redshift Serverless** | Partial — RMS + Iceberg reads | Low | RPU minimum | Moderate | Excellent | Sub-second | External only |
| **DuckDB on Lambda / MotherDuck** | Full | Low | ~$0 / low | Low–moderate | Per-Lambda / managed | 1–3 s | Maturing |
| **ClickHouse Cloud** | Full if you lakehouse it | Low | Idle billing still non-zero | Moderate | Excellent | Sub-second | Yes (external tables) |
| **BigQuery + BigLake** | Full | None | $0 | $6.25/TB on-demand | Excellent | <2 s | Yes |
| **Self-hosted Trino / StarRocks / Doris** | Full | **High** — you run the cluster | Cluster cost | Cheapest per-TB at sustained load | Excellent | N/A | Yes |

### 5.2 The contenders

**Snowflake — the product to defeat.**
Mature SQL dialect, excellent concurrency, outstanding developer ergonomics, Time Travel, Zero-Copy Cloning, Secure Data Sharing. Everything a warehouse should be. The case against it is economic and structural:
- *Cost.* Warehouses are billed per second while running, with a 60-second minimum on resume. In practice, most organizations cannot keep warehouses off — either because of BI tools, scheduled jobs, or human analysts. Storage is marked up over S3, and egress is charged. The marginal cost of one more query is low; the steady-state cost of "having a warehouse at all" is not.
- *Data ownership.* Historically, Snowflake stored tables as proprietary micro-partitions in their account. Snowflake Iceberg Tables (GA 2024) now let you write Iceberg files into your own S3 — a major step. But you're still pushed toward their catalog (Polaris or internal), and the compute model remains Snowflake-specific.
- *Lock-in.* SQL dialect differences, proprietary features (Snowpark, Streams, Tasks, Dynamic Tables), and the warehouse cost model all compound into high switching cost. "Own the data" mitigates this, but only if you've been disciplined about not using the proprietary surface.

Where Snowflake still wins decisively: hundreds of concurrent BI users, cross-organization data sharing, mature governance (masking policies, row access, tag-based classification), and institutional SQL expertise.

**Databricks SQL / Photon on Unity Catalog.**
Delta Lake or Delta + Iceberg via UniForm. Strong performance from the Photon engine. Unity Catalog is genuinely good governance. Trade-off is similar to Snowflake: a platform commitment, warehouse-style billing for SQL workloads, and a catalog that isn't Iceberg-native (UniForm is a translation layer). If your shop already runs Spark on Databricks, SQL on Databricks is the path of least resistance.

**Amazon Athena (Trino).**
The closest native-AWS peer to this stack. Truly serverless, reads S3 and Iceberg, no cluster to manage, $5/TB scanned. Great default for SQL-first teams. Where it loses to chDB-on-Lambda:
- No control over caching. Every query pays the full scan.
- Latency on small queries is dominated by metadata planning and worker startup.
- Cost scales directly with bytes scanned, which punishes iterative exploration of the same window.
- Trino dialect is standard but lacks ClickHouse's analytical function breadth (`quantileTDigest`, `uniqHLL12`, `multiIf`, regex matching with the `re2` engine).

Athena is the right choice if you want the simplest possible "SQL over S3" experience and can live with per-query scan billing. This stack is the right choice if you want low-latency iterative analysis on a hot window and the ClickHouse function surface.

**Redshift Serverless / Redshift Spectrum.**
Leased compute with an AWS-flavored warehouse. Cheaper than Snowflake at sustained volume, more operational than Athena, less open than this stack. Redshift Managed Storage is proprietary; Iceberg support is for external reads. Pick it if you're already deep in the AWS data stack and need warehouse-style concurrency without Snowflake's price tag.

**DuckDB on Lambda / MotherDuck.**
This stack's closest sibling. DuckDB's Iceberg reader is maturing fast; MotherDuck adds a managed cloud layer with smart local/remote execution. Benchmarks are comparable to chDB for single-node analytical workloads. Real selection criteria:
- *Dialect.* DuckDB: Postgres-flavored. chDB: ClickHouse-flavored. Neither is better; both are excellent.
- *Function surface.* ClickHouse's analytical functions are broader and more battle-tested on web-scale log data. DuckDB's SQL ergonomics are more familiar to Postgres users.
- *Extensions.* DuckDB's extension ecosystem (HTTPFS, Parquet, Iceberg, JSON, spatial, full-text) is the most active in this tier. chDB inherits ClickHouse's larger formats list and its JSON/dict types.

If you're starting fresh, pick the dialect your team prefers. The architectural story is identical.

**ClickHouse Cloud.**
The same engine, managed. You trade the Lambda cost envelope for a provisioned service with real concurrency, materialized views, projections, and merge-tree features chDB-on-Lambda can't offer. If your workload is "dashboards hitting the same tables all day," ClickHouse Cloud with Iceberg external tables is often the right answer. If your workload is "interactive exploration with long idle periods," Lambda wins on cost floor.

**BigQuery + BigLake.**
Google's answer. On-demand pricing at $6.25/TB, or BigQuery Editions for predictable costs. BigLake gives Iceberg/Delta reads over GCS and (with cross-cloud) S3. Excellent product. The showstopper for AWS-native shops is cross-cloud egress and the operational weight of running analytics in one cloud when everything else is in another.

**Self-hosted Trino / StarRocks / Apache Doris.**
Lowest per-TB cost at sustained high volume, strongest concurrency, best control. You pay for it with cluster operations, capacity planning, version upgrades, and on-call. Right answer for shops with either a platform team that wants the control, or sustained query volume high enough that managed services become genuinely expensive.

---

## 6. Why This Stack vs. Snowflake

The argument, in four parts.

**1. Data ownership that survives a vendor change of heart.**
Snowflake Iceberg Tables are a real improvement, but the operational posture is still "Snowflake manages your Iceberg files on your behalf." With this stack, the Iceberg metadata and data files are in S3 buckets in your account, written by your Lambda, readable by any engine on day one. The migration story if we left AWS entirely is "point a Spark/Trino/DuckDB cluster at the bucket." There is no export step because there is no import step.

**2. A cost floor that is actually zero.**
Snowflake's smallest warehouse billed for one minute costs pennies; multiplied across a thousand scheduled queries a day, it stops being pennies. The real number in most orgs is a warehouse that is effectively always on because *somebody* is always querying. This stack's idle cost is S3 storage plus whatever the S3 Tables table-bucket costs — there is no per-second compute minimum to amortize.

**3. Zero data-layout operations.**
No clustering keys to design. No `OPTIMIZE` to schedule. No `VACUUM` window. No warehouse sizing to tune. Iceberg compaction is a background job S3 Tables runs for you. chDB has no persistent state to maintain — the engine is stateless between invocations. The operational surface area is: S3 lifecycle policies, IAM, and Lambda logs.

**4. Where Snowflake still wins, and we acknowledge it.**
- **High-concurrency BI.** If 200 analysts hit the same dashboards simultaneously, a warehouse will absorb that load more gracefully than per-invocation Lambdas. Provisioned concurrency closes part of the gap; it doesn't close all of it.
- **Cross-org data sharing.** Snowflake Secure Data Sharing is a genuine feature with no real open-source equivalent.
- **Governance and lineage.** Masking policies, row access policies, tag-based classification, account-to-account replication, end-to-end lineage — Snowflake ships these. This stack doesn't, and rebuilding them is non-trivial.
- **Institutional SQL expertise.** Your analysts know Snowflake SQL. They don't know chDB's ClickHouse dialect. Training is a real cost.

The right read is not "replace Snowflake wholesale." It's "move the workloads that don't need Snowflake's concurrency and governance onto a stack you own, and shrink Snowflake down to the workloads that actually exploit what you're paying for."

---

## 7. The Future of Serverless OLAP

### 7.1 The unbundling is real

For two decades, a data warehouse meant a single vendor owned your storage format, your catalog, and your compute. That bundle is coming apart. Each layer is becoming independently addressable:

| Layer | Open options emerging |
|-------|------------------------|
| **Storage format** | Apache Iceberg, Apache Parquet, Apache Hudi, Delta Lake |
| **Catalog** | AWS Glue, AWS S3 Tables (REST Iceberg), Apache Polaris, Databricks Unity, Project Nessie, Snowflake Polaris, LakeFS |
| **Compute** | chDB, DuckDB, ClickHouse, Trino, Spark, Flink, Athena, Snowflake, Databricks, BigQuery |

The interesting market moves in 2024–2026 all push in this direction: Snowflake's Polaris (open catalog), Databricks's Unity Catalog open-sourcing pieces, AWS S3 Tables shipping a managed REST Iceberg catalog, Apache Iceberg's REST spec stabilizing, `DELTA <-> Iceberg` UniForm bridging, and the rapid maturation of embedded engines (DuckDB, chDB, Velox-based engines).

### 7.2 S3 Tables as a managed primitive

AWS S3 Tables is an explicit bet that *the catalog* is the next managed AWS primitive, sitting next to S3 itself. The bucket-policy friction we hit with chDB's `icebergS3()` is the rough edge of a young product — expect it to close within a year. When it does, the Glue mirror in this repo is deletable, and the architecture simplifies to a single catalog.

More broadly, S3 Tables's bet is that compaction, snapshot expiration, and manifest maintenance are undifferentiated heavy lifting that customers shouldn't run. That's correct — those are exactly the ops that get skipped until they break.

### 7.3 Embedded engines on FaaS are credible for most analytics

Embedded analytical engines — chDB, DuckDB, Velox-based ones — have closed the gap with traditional MPP engines for single-node workloads. A single Lambda with 3 GB of memory and chDB can comfortably answer most dashboard queries on datasets up to ~100 GB of working set per query. Above that, the boundary shifts to provisioned compute (ClickHouse Cloud, Trino, Spark).

The implication: "analytics" as a workload category is bifurcating. The interactive / iterative / irregular majority fits on FaaS + object storage. The sustained / high-concurrency / batch minority continues to want provisioned compute. Both can share the same Iceberg bytes.

### 7.4 Prediction

Within eighteen months, the "warehouse vs. lakehouse" framing will collapse. The meaningful axis will be:

> **Managed catalog + open format + bring-your-own-compute**

The winners will be whoever removes the last operational burdens — compaction, statistics, GC, concurrent-writer conflict resolution — without owning the data. That's the posture this repo is built around, and it's the posture AWS is moving toward with S3 Tables. Snowflake's Polaris play and Databricks's Unity open-sourcing are answers to the same question: *how do we keep customers when the data isn't locked in anymore?* The answer turns out to be "be the best place to run compute against data you don't own," and that's a much more competitive market than the one Snowflake grew up in.

---

## 8. When to Pick What — A Recommendation Matrix

| Workload shape | Recommended |
|---|---|
| Ad-hoc exploration on hot window, <10 GB / query, irregular volume | **This stack** (chDB + S3 Tables + Lambda) |
| Iterative analysis on logs / events, ClickHouse dialect comfortable | **This stack** or ClickHouse Cloud |
| SQL-first team, "just let me query S3," can tolerate per-TB scan billing | **Athena** |
| 200+ concurrent BI users on shared dashboards | **Snowflake**, **Databricks SQL**, or **ClickHouse Cloud** |
| Cross-organization data sharing | **Snowflake** |
| Already on Databricks for ETL, want SQL alongside | **Databricks SQL** |
| PB-scale batch + interactive on the same data | **Spark on Iceberg** + one of the serverless engines for interactive |
| Sustained high-volume query load, platform team exists | **Self-hosted Trino / StarRocks / Doris** |
| Postgres-dialect preference, same architectural story | **DuckDB on Lambda / MotherDuck** |
| Google Cloud shop | **BigQuery + BigLake** |

The through-line: *pick the compute to match the concurrency and latency profile, and let the data stay in Iceberg on S3.* That is the decoupling this stack demonstrates end-to-end.

---

## 9. References

**In this repo**
- `README.md` — deployment and demo walkthrough.
- `scripts/demo/README.md` — backend/engine matrix, query examples.
- `src/chdb_aws/read/query.py` — engine dispatch, cache logic, `icebergS3` constraints.
- `src/chdb_aws/write/iceberg_writer.py` — dual-catalog append.
- `terraform/modules/chdb_aws/lambda.tf` — runtime sizing and rationale.
- `terraform/modules/chdb_aws/s3_tables.tf` — S3 Tables resources.
- `terraform/modules/chdb_aws/glue.tf` — parallel Glue backend.

**External**
- chDB — https://github.com/chdb-io/chdb
- Apache Iceberg — https://iceberg.apache.org
- PyIceberg — https://py.iceberg.apache.org
- AWS S3 Tables — https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html
- Iceberg REST Catalog spec — https://iceberg.apache.org/concepts/catalog/#rest-catalog
- Snowflake Iceberg Tables — https://docs.snowflake.com/en/user-guide/tables-iceberg
- Apache Polaris — https://github.com/apache/polaris
- Databricks Unity Catalog — https://docs.databricks.com/data-governance/unity-catalog
- DuckDB Iceberg extension — https://duckdb.org/docs/extensions/iceberg.html
- ClickHouse Cloud — https://clickhouse.com/cloud
