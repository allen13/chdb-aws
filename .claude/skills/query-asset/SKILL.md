---
name: query-asset
description: Answer analytical questions about any asset defined in `terraform/main/terraform.tfvars` by reading its Iceberg schema, generating chDB SQL that is type-aware for that schema, invoking the read Lambda, and rendering the result as a markdown table. Use when the user asks a data question like "how many 5xx errors today", "top 10 paths by p95 latency", "distinct users by country", or any variant where the answer is a SQL query against one of the declared assets.
---

# query-asset

The read-side counterpart to `generate-schema`. This skill owns the loop:

> **schema discovery → SQL generation → Lambda invocation → structured table rendering**

It does **not** edit any files. It reads `terraform/main/terraform.tfvars` for the schema, invokes the deployed read Lambda, and returns a markdown table with per-query metrics. If you need to add or change a schema, use `generate-schema` instead.

## 1. When to invoke

Trigger on any of these:

- "query the `requests` table for …"
- "how many / top N / distribution of …" — any analytical question
- "p50 / p95 / p99 of …"
- "show me the last hour of …"
- "count distinct …"
- "compare X between Y and Z"
- "run this SQL for me" — user already has SQL
- "what data do we have on …" — schema exploration

If the answer requires querying data in one of the declared assets, this skill owns it.

## 2. Flow

Every invocation runs these five steps in order. Do not skip.

1. **Discover schemas.** Read `terraform/main/terraform.tfvars`. Extract the `assets` map into a table of `(asset_name, column_name, type, required)` triples.
2. **Pick the asset.** If the user named one, use it. If only one asset exists (today: `requests`), use it silently. If multiple exist and the user's question is ambiguous, ask which one.
3. **Generate chDB SQL** following §5–§7. The SQL template must reference the table as `${asset}` — the Lambda substitutes the real table function at query time.
4. **Invoke the read Lambda** via §8. Parse the JSONCompact response.
5. **Render** as a markdown table with a metrics line (§9). Include the SQL for transparency.

## 3. Discovering the schema

`terraform/main/terraform.tfvars` is the source of truth. Parse the `assets = { … }` block. Each asset has:

```hcl
<asset_name> = {
  schema = [
    { name = "<col>", type = "<iceberg_type>", required = true },
    { name = "<col>", type = "<iceberg_type>" },
  ]
}
```

If the file is malformed or the asset the user named isn't present, stop and report which assets *are* defined — don't guess.

### Current assets

At the time of writing, `terraform.tfvars` defines one asset:

| Asset | Columns |
|---|---|
| `requests` | `request_id:string!`, `ts:timestamptz!`, `edge_pop:string`, `client_ip:string`, `country:string`, `http_method:string`, `path:string`, `status_code:int`, `bytes_sent:long`, `response_time_ms:int`, `cache_hit:boolean`, `user_agent:string`, `referrer:string` |

(`!` = `required = true`.) Always re-read the tfvars — this list is a snapshot, not a contract.

## 4. Iceberg → chDB type mapping (for SQL generation)

chDB reads Iceberg types through its `icebergS3()` table function; the resulting types in SQL are ClickHouse types. Use this mapping to choose functions:

| Iceberg | chDB behaviour | SQL notes |
|---|---|---|
| `boolean` | `Bool` | `avg(col) * 100` gives rate in percent; `countIf(col)` for counts |
| `int` (32-bit signed) | `Int32` | use for status codes, small counters |
| `long` (64-bit signed) | `Int64` | use for bytes, IDs, large counters |
| `float` / `double` | `Float32` / `Float64` | prefer `quantile()` / `avg()`; beware NaN comparisons |
| `decimal(P,S)` | `Decimal(P,S)` | preserves exactness; `sum`/`avg` safe |
| `date` | `Date` | `toStartOfMonth`, `today()`, `yesterday()` |
| `time` | `Time` (microseconds) | rarely used; format with `formatDateTime` |
| `timestamp` | `DateTime64(6)` | no zone; use for naive wall-clock values |
| `timestamptz` | `DateTime64(6, 'UTC')` | **preferred for event time**. All of `toStartOfHour`, `toStartOfInterval(ts, INTERVAL 15 MINUTE)`, `dateDiff`, `INTERVAL` work |
| `string` | `String` | `like`, `match()` for regex, `lower()`, `upper()`, `splitByChar` |
| `uuid` | `UUID` | `toString(col)` to print, `equals` to compare |
| `binary` / `fixed(L)` | `String` (bytes) | `hex()` to render |
| `list<T>` | `Array(T)` | `arrayJoin` to unnest, `length` for size, `has` for membership |
| `map<K, V>` | `Map(K, V)` | `m['key']` lookup, `mapKeys`/`mapValues` |
| `struct<…>` | `Tuple(…)` | `col.field_name` projection (chDB ≥ 4.0) |

## 5. SQL generation guidelines

Always write SQL that is:

- **Case-sensitive correct.** Column names are lowercase (§3); match exactly.
- **Templated.** Reference the table as `${asset}` literally — the Lambda injects the table function.
- **Projection-light.** Select only what the answer needs. Every byte not returned is money saved.
- **Bounded.** Free-text top-N → `LIMIT N`; time-range questions → explicit `WHERE ts >= …`.
- **Typed.** Round floats to a sensible precision (`round(x, 2)`), format timestamps (default `DateTime64(6, 'UTC')` renders fine, so leave as-is unless the user asks).
- **Single statement.** The handler expects one SQL string; no `;`-separated batches.

### 5.1 Common patterns

| Question shape | Pattern |
|---|---|
| Total / count | `SELECT count() FROM ${asset}` |
| Distinct count | `SELECT uniqExact(col) FROM ${asset}` (exact) or `uniq(col)` (HLL, cheaper) |
| Top N by X | `SELECT col, count() AS c FROM ${asset} GROUP BY col ORDER BY c DESC LIMIT N` |
| Share of total | `round(count() * 100.0 / sum(count()) OVER (), 2) AS share_pct` |
| Time bucketing | `toStartOfHour(ts)`, `toStartOfInterval(ts, INTERVAL 15 MINUTE)`, `toStartOfDay(ts)` |
| Quantiles | `quantile(0.95)(col)` or family: `quantiles(0.5, 0.9, 0.95, 0.99)(col) AS qs` |
| Conditional agg | `countIf(cond)`, `avgIf(col, cond)`, `sumIf(col, cond)` |
| Labelled bucketing | `multiIf(cond1, 'label1', cond2, 'label2', …, 'other')` |
| Threshold CTE | `WITH (SELECT quantile(0.99)(col) FROM ${asset}) AS t SELECT … WHERE col > t` |
| Regex match | `match(str, '(?i)pattern')` — chDB uses re2 syntax |
| Substring | `positionCaseInsensitive(str, 'needle') > 0` (faster than regex) |
| Last 24h of data | `WHERE ts >= (SELECT max(ts) FROM ${asset}) - INTERVAL 24 HOUR` (data-relative, not `now()`-relative — demo data may be stale) |

### 5.2 Precision and rounding defaults

- Rates: `round(avg(bool) * 100, 1) AS rate_pct`
- Averages of timings: `round(avg(col), 1) AS avg_ms`
- Quantiles of timings: `round(quantile(0.95)(col), 1) AS p95_ms`
- Share / percent: `round(expr, 2) AS share_pct`
- Bytes: `formatReadableSize(sum(bytes_sent))` when rendering for humans, raw `sum` when the user will do more math

### 5.3 Things that commonly go wrong

- **`now()` vs `max(ts)`.** Demo data is static. For "last 24h", anchor to `max(ts)`, not `now()`, unless the user is querying live ingestion.
- **Timezone in `timestamp` vs `timestamptz`.** `ts` on `requests` is `timestamptz` → chDB sees `DateTime64(6, 'UTC')`. Date math is straightforward; comparisons to string literals need `toDateTime64('2025-01-01 00:00:00.000000', 6, 'UTC')`.
- **Reserved keywords.** None of the `requests` columns hit these, but future schemas might. Double-quote identifiers (`"timestamp"`) if needed.
- **`client_ip` cardinality.** High-cardinality `GROUP BY` (~hundreds of thousands of uniques on the demo data) is slow. Add a filter (e.g. only errored requests) or `LIMIT` before grouping when possible.
- **Empty strings vs NULL.** Optional string columns often hold `''` rather than `NULL`. Filter with `col != ''` when the user means "where country is set".

## 6. Pre-query validation

Before invoking the Lambda, check every item. Stop and ask if anything fails:

- [ ] Asset name exists in `terraform.tfvars`
- [ ] Every column referenced in the SQL exists in that asset's schema
- [ ] Every function used matches the column's type family (§4)
- [ ] `${asset}` placeholder is present (not replaced with a hardcoded name)
- [ ] SQL is a single statement with no trailing `;`
- [ ] If the question has a time axis, it uses the asset's timestamp column (for `requests`, that's `ts`)
- [ ] Any user-supplied string literals inside the SQL are single-quoted and free of unescaped single quotes

## 7. Backend and engine selection

The Lambda accepts `{backend, engine}` per `src/chdb_aws/read/handler.py:16-19`. Defaults:

| Parameter | Lambda default | This skill's default | Rationale |
|---|---|---|---|
| `backend` | `s3tables` | **`glue`** | chDB's native `icebergS3()` reads Glue-backed Iceberg directly; S3 Tables blocks `GetBucketLocation` (see `src/chdb_aws/read/query.py:197-210`). |
| `engine`  | `materialize` | **`iceberg_s3`** | No `/tmp` materialization, no per-snapshot cliff; scales to arbitrary table sizes. |

Override only when:

- **Use `engine=materialize`** if the user says "run a bunch of queries against this snapshot" — the first query pays the materialize cost, then every subsequent one is sub-second against the `/tmp` cache.
- **Use `engine=scan` with `--column` / `--where`** if the query touches a narrow slice of a wide table and the user is cost-sensitive. Note that `where` is *additional* to the SQL's `WHERE` — pyiceberg pushdown happens before chDB ever sees the data.
- **Use `backend=s3tables`** only if the user explicitly wants to validate the S3 Tables path (testing / comparison); otherwise never.

## 8. Invoking the Lambda

Use the existing `scripts/query.py` — it handles payload shape, error propagation, and raw-JSON stdout. The read Lambda returns **JSONCompact** format, which is a JSON object: `{"meta": [{"name": "...", "type": "..."}, ...], "data": [[row1values], ...], "rows": N, "statistics": {"elapsed": s, "rows_read": N, "bytes_read": N}}`.

### 8.1 Discover the Lambda function name

```bash
FUNCTION_NAME=$(terraform -chdir=terraform/main output -raw read_lambda_function_name 2>/dev/null || echo "chdb-aws-prod-read")
```

Fall back to the literal `chdb-aws-prod-read` (the prod convention; matches `_helpers.py:69`).

### 8.2 Invoke

```bash
uv run scripts/query.py \
    --function-name "$FUNCTION_NAME" \
    --asset requests \
    --backend glue \
    --engine iceberg_s3 \
    --sql "$(cat <<'SQL'
SELECT
    country,
    count() AS requests,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS share_pct
FROM ${asset}
WHERE country != ''
GROUP BY country
ORDER BY requests DESC
LIMIT 10
SQL
)"
```

Use a `<<'SQL'` heredoc (single-quoted terminator) so `${asset}` is not interpolated by the shell. stdout is the raw JSONCompact body.

### 8.3 Handling errors

- **Non-200 body** → `scripts/query.py` exits 1 and prints `statusCode=…, body=…` to stderr. Surface that message verbatim; do not retry blindly.
- **`Lambda error: …`** → the handler raised (malformed SQL, unknown asset, type mismatch). Read the error, fix the SQL, ask the user only if the fix isn't obvious.
- **`iceberg_s3 + backend=s3tables is unsupported`** → you set `backend=s3tables` while engine was `iceberg_s3`. Switch to `backend=glue` or change the engine.

## 9. Rendering the result

Parse the JSONCompact stdout and emit:

1. A fenced `sql` block with the exact SQL you ran.
2. A markdown table with the result rows. Header cells use the column names from `meta`.
3. A single-line metrics footer.

### 9.1 Formatting rules

| Column type (from `meta[i].type`) | Rendering |
|---|---|
| `Int*`, `UInt*` | thousands-separated: `1,234,567` |
| `Float*`, `Decimal*` | thousands-separated, 2 decimals: `12,345.67`; if already rounded in SQL, preserve that precision |
| `DateTime*`, `Date` | ISO string as returned (`2025-01-15 14:30:00.000000`); strip trailing zeros only if all rows share them |
| `String`, `UUID` | verbatim; truncate to 60 chars with `…` if wider, and note the truncation in the metrics line |
| `Array*`, `Map*`, `Tuple*` | JSON-compact form |
| `NULL` (any type) | render as `—` (em dash) |

Right-align numeric columns in the markdown table by adding `---:` or `:---:` in the header separator. Left-align strings and timestamps.

### 9.2 Metrics footer

One line, under the table:

> `wall <ms> · chdb <ms> · rows read <N> · bytes read <B> · returned <R> rows`

Where:
- `wall` = measured in the skill (or reported by `scripts/query.py` via `--time` if available; otherwise omit and say so)
- `chdb`, `rows read`, `bytes read` from `statistics` in the response
- `returned` from `rows` in the response

### 9.3 Size limits

If the result has > 50 rows, render the first 50 and add a line: `… (showing 50 of N rows; re-query with LIMIT N to see more)`. If a row is wider than 8 columns, consider asking whether the user wants a narrower projection.

## 10. Worked examples

### 10.1 Simple count

**User:** "how many requests have we got?"

Schema lookup → `requests` has `request_id string!`. Generate:

```sql
SELECT count() AS n FROM ${asset}
```

Invoke. Render:

```
| n         |
| --------: |
| 1,000,000 |
```

> wall 1,420 ms · chdb 87 ms · rows read 1,000,000 · bytes read 4.0 MiB · returned 1 row

### 10.2 Top countries with share-of-total

**User:** "what are the top 10 countries by traffic share?"

```sql
SELECT
    country,
    count() AS requests,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS share_pct
FROM ${asset}
WHERE country != ''
GROUP BY country
ORDER BY requests DESC
LIMIT 10
```

Render as a 3-column markdown table. Right-align `requests` (Int) and `share_pct` (Float).

### 10.3 Latency quantile family on errored requests

**User:** "what's the p50/p95/p99 response time for 5xx errors?"

```sql
SELECT
    round(quantile(0.50)(response_time_ms), 1) AS p50_ms,
    round(quantile(0.95)(response_time_ms), 1) AS p95_ms,
    round(quantile(0.99)(response_time_ms), 1) AS p99_ms,
    count() AS errors
FROM ${asset}
WHERE status_code >= 500
```

Single row out. Metrics footer shows how many rows chDB had to scan.

### 10.4 Time-bucketed error rate, last 24h of data

**User:** "show me the 5xx rate per 15 minutes over the last day of data"

```sql
SELECT
    toStartOfInterval(ts, INTERVAL 15 MINUTE) AS bucket,
    count()                                   AS requests,
    countIf(status_code >= 500)               AS errors,
    round(countIf(status_code >= 500) * 100.0 / count(), 2) AS error_pct
FROM ${asset}
WHERE ts >= (SELECT max(ts) FROM ${asset}) - INTERVAL 24 HOUR
GROUP BY bucket
ORDER BY bucket
```

Note the `max(ts)` anchor — demo data is static.

### 10.5 User already has SQL

**User:** "run this: `SELECT cache_hit, count() FROM ${asset} GROUP BY cache_hit`"

Skip SQL generation, go straight to §6 validation → §8 invocation. Render the result. If the user used `FROM requests` literally (no `${asset}`), rewrite to `${asset}` before invoking — the Lambda requires the template.

### 10.6 Ambiguous asset

**User:** "show me the top 10 by volume" *(and two assets are defined)*

Ask: "Which asset — `requests` or `<other>`?" Don't guess.

## 11. What this skill does not do

- **Write or change schemas** → use `generate-schema`.
- **Populate test data** → `scripts/demo/populate.py`.
- **Queries that span multiple assets** — the Lambda takes one `asset` at a time. For cross-asset analysis, materialize intermediate results to a parquet under `dropzone/` of a new asset, or ask the user to define a combined asset.
- **Write results anywhere** — output is always a markdown table in the response. If the user wants a file, they'll say so.

## 12. Quick reference — Lambda payload shape

```json
{
  "asset":   "requests",
  "sql":     "SELECT ... FROM ${asset} ...",
  "backend": "glue",
  "engine":  "iceberg_s3",
  "columns": ["status_code"],
  "where":   "status_code >= 500"
}
```

`columns` and `where` are only meaningful when `engine == "scan"`.

## 13. Validation checklist (pre-invocation)

- [ ] Schema re-read from `terraform/main/terraform.tfvars` (don't rely on cached knowledge)
- [ ] Every column referenced exists and is the right type-family for its function
- [ ] `${asset}` placeholder present, single statement, no trailing `;`
- [ ] `backend=glue, engine=iceberg_s3` unless explicitly overridden
- [ ] Function name discovered from `terraform output`, falling back to `chdb-aws-prod-read`
- [ ] SQL quoted safely into the shell (heredoc with single-quoted terminator)
- [ ] A `LIMIT` clause or a tight `WHERE` exists for unbounded top-N or scan-the-whole-table queries
