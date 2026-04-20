# Iceberg Management on S3

An operator's guide to owning Apache Iceberg tables on S3: what maintenance actually has to happen, who does it in each catalogue option, and what you trade off relative to Snowflake.

> **Scope.** This is the operational companion to [`architecture-review.md`](architecture-review.md). That doc argues *why* the open-format stack makes sense. This one is for the person who has to keep it running.

Source of truth for the tables in this repo: [`terraform/main/terraform.tfvars`](../terraform/main/terraform.tfvars).

---

## 1. Iceberg in one page

Iceberg is a **table format** — a spec for how to lay parquet files out on object storage so that a set of them behaves like a database table. It is not a storage engine, a catalogue, or a query engine. Those come from somewhere else.

### Physical layout

Every Iceberg table is four layers of files in S3:

| Layer | What it is | How many |
|---|---|---|
| Data files | Parquet with the actual rows | Thousands → millions |
| Manifests | Lists of data files + per-file column stats | One per commit batch |
| Manifest list | List of manifests in a snapshot | One per snapshot |
| `metadata.json` | Pointer to current manifest list + schema + history | One *current*, old ones retained |

```
s3://my-bucket/my-table/
  data/                 parquet
  metadata/
    00001-<uuid>.metadata.json
    00002-<uuid>.metadata.json    <- "current"
    snap-<id>.avro                <- manifest lists
    <uuid>-m0.avro                <- manifests
```

### The catalogue's job

The catalogue answers exactly one question: **"which `metadata.json` is current for this table right now?"** Everything else lives in S3 alongside the data.

Catalogues differ in how they answer that question:
- **Iceberg REST** — stateless HTTP API (the spec). AWS S3 Tables speaks it.
- **AWS Glue** — the Glue table's location + parameters point at the metadata file.
- **Hive Metastore** — legacy; same idea.
- **Proprietary** — Snowflake, Databricks Unity, Dremio Arctic each run their own.

Atomic swap of the "current pointer" is what gives Iceberg ACID semantics on object storage. Readers see a consistent snapshot; writers either succeed or don't.

### Snapshots and commits

Every write creates a new snapshot. The snapshot is immutable. The catalogue's pointer moves from the old metadata file to the new one atomically. Readers that opened against the old snapshot keep seeing the old data until they re-plan — this is **snapshot isolation**, for free.

Consequence: nothing in Iceberg ever really gets deleted at write time. "Delete" means "write a new snapshot that doesn't include those rows, and eventually garbage-collect the old snapshot." Garbage collection is not free and not automatic — see §3.

---

## 2. The table lifecycle

Six stages. Each one has a maintenance burden that someone — you, AWS, or Snowflake — has to carry.

### 2.1 Write / append

What happens: client writes parquet file(s), writes a manifest referencing them, writes a manifest list referencing the new manifest plus the old ones, writes a new `metadata.json`, and atomically tells the catalogue to point at it.

**The small-file problem is born here.** Every commit — no matter how small — produces at least one parquet file. A firehose of 1-row appends produces a firehose of 1-row parquets. Reads slow down superlinearly as file count grows, because planning has to open every manifest to decide which files to touch.

**Concurrency.** Iceberg uses optimistic concurrency: two writers can commit in parallel, but only one wins the atomic swap. The loser re-reads current metadata and retries. Works fine at low concurrency. Under contention (many writers, hot table), retries compound and commits stall.

### 2.2 Read

Readers ask the catalogue for current metadata, parse the manifest list, and use manifest stats to prune files before opening any parquet. Planning cost scales with **number of manifests**, not number of rows.

A neglected table — no compaction, no manifest rewrite — can spend more time in planning than in scanning data. You'll feel it as "why does `SELECT count(*)` take 40 seconds on a tiny table."

### 2.3 Compaction

The cure for §2.1. Bin-pack rewrite that merges small parquets into target-sized ones (typically 128–512 MB). Three flavours:

| Flavour | What it does | When to use |
|---|---|---|
| Bin-pack | Merge files to target size | Always — the baseline |
| Sort | Bin-pack + sort by chosen columns | Tables queried with a consistent predicate |
| Cluster / Z-order | Multi-dimensional ordering | Analytical tables with several hot predicates |

Sort/cluster compaction is what gives you **pruning** — the ability for planning to skip entire files because their column stats don't overlap the predicate. Without it, you read everything.

Neglected compaction → slow reads, expensive scans, unbounded manifest count. It's the single most load-bearing maintenance job.

### 2.4 Snapshot expiration

`expire_snapshots` drops snapshot pointers older than a retention window. The data files those snapshots referenced are only removed if no *other* snapshot references them.

Without expiration, storage grows without bound. Every compaction rewrites files; the pre-compaction files live forever in old snapshots. A heavily maintained table can spend 10× its live data on retained snapshot history.

Retention window is a policy call: you want enough window to support time travel and recovery (typically 7–30 days), but not so much that storage blows up.

### 2.5 Orphan file cleanup

`remove_orphan_files` deletes files in the table's S3 prefix that no manifest references. Orphans happen when writes fail partway through, or when a non-Iceberg process writes into the prefix.

**This is the maintenance job most likely to destroy data if you get it wrong.** Always run with a grace period (24–72 hours minimum) so in-flight commits have a chance to land. Never run it on a table you don't fully own the S3 prefix for.

### 2.6 Manifest rewrite and statistics

Separate from data-file compaction: over time, manifest churn — lots of small manifests each referencing a handful of files — makes planning slow even when the data files themselves are fine. `rewrite_manifests` merges manifests.

Statistics (`ANALYZE`-equivalent): per-column NDV, histograms, and theta/HLL sketches stored in statistics files, consumed by cost-based optimizers in Trino/Spark/Snowflake. Iceberg supports the statistics file format; most engines don't write them yet. Absence shows up as bad join plans on large tables.

---

## 3. Why Snowflake became the default

Snowflake's pitch is not "a query engine." It's "we will operate a warehouse so you don't have to." Concretely, the things on that list that you're about to inherit:

- **Automatic compaction.** Micro-partitions (~16 MB chunked units) are merged and re-clustered continuously, invisibly. You do not tune it. You do not schedule it.
- **Automatic clustering.** Declare a clustering key; Snowflake maintains it. Pruning just works.
- **Automatic statistics.** Used by the cost-based optimizer for every query. You don't run `ANALYZE`.
- **Automatic snapshot management.** Time travel and fail-safe windows are configured by retention policy, not by you running `expire_snapshots`.
- **Result cache + metadata cache.** Identical queries return instantly. Plan cache survives across warehouses.
- **Materialized views and search-optimization indexes.** Maintained incrementally and automatically.
- **Zero-setup multi-writer.** Concurrent DML at scale without the user thinking about optimistic-concurrency retries.
- **Zero-copy clones.** Metadata-only forks of a table for dev/test. Cheap.
- **Secure data sharing.** Cross-account read access without copying data.

This is the ambient service level open-format users quietly benefit from when their Snowflake DBA is handling it, and quietly inherit when they leave.

**What you keep by leaving:** bucket ownership, format portability, elastic compute decoupled from storage, per-query billing transparency, the ability to use any Iceberg-capable engine against the same tables.

**What you must replace or consciously drop:** every bullet above.

---

## 4. Options for Iceberg on S3

Five rows — the three the team named plus two notable reference points. All use Iceberg-format data in S3; the differences are in the catalogue and in who runs the maintenance jobs from §2.

| Option | Catalogue | Auto compaction | Snapshot expiration | Orphan cleanup | Statistics / indexing | Schema evolution | Multi-writer | Engine access | Cost model | Data ownership |
|---|---|---|---|---|---|---|---|---|---|---|
| **Snowflake-managed Iceberg** | Snowflake (internal) or external (Glue / Polaris) [¹](#fn1) | Yes, invisible | Yes, automatic | Yes, automatic | Automatic clustering + search-opt indexes | Full Iceberg | Yes, at scale | Snowflake-first; any engine if external catalogue | Storage + credits (warehouses) | Your bucket (external) or Snowflake's (internal) |
| **AWS S3 Tables** | Managed Iceberg REST | Yes, background | Yes, background | Yes, background | None beyond Iceberg file stats | Full Iceberg | Optimistic via REST | Anything speaking REST | Storage + per-op + compaction | Your account, but bucket policy is service-managed [²](#fn2) |
| **Glue catalogue, self-managed** | AWS Glue | **None** — you run it | **None** — you run it | **None** — you run it | None automatic | Full Iceberg | Optimistic via Glue locks | Any Iceberg engine | Storage + your compute | Full — standard S3 bucket |
| **Apache Polaris / Nessie** (OSS) | Iceberg REST, self-hosted | **None** — you run it | **None** — you run it | **None** — you run it | None automatic | Full Iceberg (Nessie adds branching) | Yes, via catalogue | Any Iceberg engine | Storage + your compute + catalogue host | Full — standard S3 bucket |
| **Databricks Unity Catalog** | Unity (proprietary) | Yes, Predictive Optimization | Yes, managed | Yes, managed | Auto stats + liquid clustering | Full Iceberg (via UniForm) | Yes, at scale | Databricks-first; Iceberg read via UniForm | Storage + DBUs | Your bucket; Unity governs access |

<a id="fn1"></a>**[1]** Snowflake has two modes for Iceberg: *Snowflake-managed* (Snowflake is the catalogue; other engines read via Snowflake's catalogue REST) and *externally managed* (Glue / Polaris is the catalogue; Snowflake reads but doesn't write). Only the managed mode gives you the full auto-maintenance experience.

<a id="fn2"></a>**[2]** S3 Tables stores data in service-managed `*--table-s3` buckets in your account. You own the bytes, but the bucket policy is controlled by AWS — which is why [`src/chdb_aws/read/query.py:197-220`](../src/chdb_aws/read/query.py) has to reject `icebergS3() + backend=s3tables`: the bucket policy denies `GetBucketLocation`, and ClickHouse calls it unconditionally.

### How to read the table

- **Three "Yes, automatic" rows** (Snowflake, S3 Tables, Unity) are the managed options. You pay for the service; maintenance is not your problem.
- **Two "None — you run it" rows** (Glue self-managed, Polaris/Nessie) are the bring-your-own-ops options. Cheaper on paper, and you get the most control, but the checklist in §6 is now yours.
- The **Iceberg format is identical across every row.** You can switch catalogues later; you cannot cheaply switch off Snowflake-managed-internal (the data lives in Snowflake's storage).

---

## 5. What this repo does today

Both managed and self-managed paths are wired up in parallel, so the team can compare them on real workloads without a migration.

### S3 Tables path (managed)

- [`terraform/modules/chdb_aws/s3_tables.tf:1-32`](../terraform/modules/chdb_aws/s3_tables.tf) declares the table bucket, namespace, and one `aws_s3tables_table` per asset in `terraform.tfvars`. Schema lives in Terraform.
- [`src/chdb_aws/write/iceberg_writer.py:32-44`](../src/chdb_aws/write/iceberg_writer.py) loads the catalogue via pyiceberg REST with SigV4 signing.
- AWS runs compaction, snapshot expiration, and orphan cleanup in the background. **No maintenance code in this repo.**

### Glue path (traditional / self-managed)

- [`terraform/modules/chdb_aws/glue.tf:1-37`](../terraform/modules/chdb_aws/glue.tf) provisions a regular S3 bucket + Glue database. Tables are **not** declared in Terraform — see the comment at the top of the file for why (`aws_glue_catalog_table` collapses Iceberg types to Hive types and breaks `required` flags).
- [`src/chdb_aws/write/iceberg_writer.py:47-71`](../src/chdb_aws/write/iceberg_writer.py) creates the Glue table lazily on first write, mirroring the s3tables schema.
- [`src/chdb_aws/write/iceberg_writer.py:74-82`](../src/chdb_aws/write/iceberg_writer.py) is the dual-write: every append goes to s3tables *and* Glue. If either fails, the source parquet stays in the dropzone for retry.

### Read-path asymmetry

[`src/chdb_aws/read/query.py:197-220`](../src/chdb_aws/read/query.py) documents why chDB's native `icebergS3()` only works against the Glue backend: ClickHouse calls `GetBucketLocation` before any S3 read, and the S3 Tables service bucket policy denies it. Materialize and scan engines work against both.

### Gaps — known, not fixed

- **No compaction on the Glue side.** Small-file accumulation is already measurable on the demo data. S3 Tables compacts automatically; Glue does not.
- **No snapshot expiration anywhere in the repo.** S3 Tables handles its own; the Glue-side `metadata/` directory grows unbounded.
- **No orphan file cleanup anywhere.** Lower priority (the write Lambda is currently the only writer and commits are atomic), but still a gap.

---

## 6. Migration readiness checklist

Before replacing Snowflake with any combination of the options above, the team needs to own answers to all of these:

- [ ] **Compaction.** Scheduled job with a frequency (hourly / daily), concurrency cap, and a cost budget. For self-managed Glue: pick a runner (Spark on EMR / Athena CTAS / a PyIceberg job in Lambda). For S3 Tables: confirm the managed cadence meets latency requirements.
- [ ] **Snapshot expiration policy.** Retention window in both *number of snapshots* and *wall-clock hours*. Longer → more time-travel coverage and more storage cost.
- [ ] **Orphan file cleanup.** Grace period of at least 24h. Must run with a lock preventing concurrent writes, or with the writer quiesced. Dry-run for first N runs.
- [ ] **Statistics / clustering strategy.** Identify the top 3–5 query patterns; pick clustering columns and a sort strategy per table. Decide whether statistics files are worth writing for the engines you use.
- [ ] **Monitoring.** At minimum: commit latency p95, manifest count per table, file count per partition, orphan-file-candidate count. Alert on manifests-per-table exceeding a threshold — that's your "compaction is behind" signal.
- [ ] **Catalogue backup / DR.** Glue: back up the catalogue database. Polaris/Nessie: back up the catalogue's own storage. S3 Tables / Snowflake: this is on them.
- [ ] **Multi-writer invariant.** Either commit to a single-writer model (and enforce it — the write Lambda is currently this) or test optimistic-concurrency behaviour under your real write pattern.
- [ ] **Access control model.** Write access, read access, and *expiration authority* are three different things. Whoever can run `expire_snapshots` can destroy time-travel history; scope it tightly.

Any box still unchecked when you cut over is an incident waiting to file itself.

---

## 7. Further reading

- [`architecture-review.md`](architecture-review.md) — strategic positioning of this stack vs. Snowflake / Databricks / Athena.
- [Iceberg spec](https://iceberg.apache.org/spec/) — the canonical reference for the format.
- Apache Iceberg table maintenance procedures: `rewrite_data_files`, `rewrite_manifests`, `expire_snapshots`, `remove_orphan_files`.
