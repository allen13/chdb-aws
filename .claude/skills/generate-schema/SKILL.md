---
name: generate-schema
description: Add, modify, or remove Iceberg-backed S3 Tables for this chdb-aws project by editing `terraform/main/terraform.tfvars` directly. Handles one or many assets per invocation. Use when the user wants to create a new asset, change fields on an existing asset, drop an asset, convert a parquet/JSON/PyArrow sample into a schema, or evolve a schema safely.
---

# generate-schema

Authoritative skill for maintaining the schema lifecycle of this project's parquet → S3 Tables (Iceberg) → chDB pipeline. **The skill edits `terraform/main/terraform.tfvars` itself** — it does not merely print HCL for the user to copy.

## 1. When to invoke

Trigger on any of these:

- "add an asset called …", "create a table for …", "new S3 table …"
- "change the schema of …", "add a column to …", "make X required", "widen …"
- "drop the … asset", "remove …" (destructive — see §11)
- "here's a parquet file / PyArrow schema / JSON sample, turn it into a table"
- "review the schema before I apply"

If the user's request touches `terraform/main/terraform.tfvars` `assets` map, this skill owns the edit.

## 2. Why schemas in this project are load-bearing

The schema in `terraform/main/terraform.tfvars` must simultaneously satisfy four consumers — there is no runtime validation layer that softens the blow of a mismatch:

| Consumer | Where it binds | Failure mode |
|---|---|---|
| Terraform → AWS S3 Tables REST catalog | `terraform/modules/chdb_aws/s3_tables.tf:10` (`aws_s3tables_table` with `for_each = var.assets`) | `terraform apply` errors or creates a wrong-shape table |
| PyArrow reading dropzone parquet | `src/chdb_aws/write/handler.py:37` (`pq.read_table(io.BytesIO(body))`) | Lambda throws on malformed parquet |
| pyiceberg appending to the table | `src/chdb_aws/write/iceberg_writer.py:26` (`table.append(rows)`) | Runtime exception on PyArrow↔Iceberg type mismatch |
| chDB reading via `icebergS3()` | `src/chdb_aws/read/query.py:41` | Query fails or returns wrong types |

Because the module already uses `for_each = var.assets`, **every new key in the `assets` map becomes a new S3 table**. Creating "multiple tables with multiple schemas" = adding multiple keys to the map in one edit.

## 3. Target file and exact HCL shape

Edit **`terraform/main/terraform.tfvars`**. The existing `events` entry (lines 6-15) is the canonical style; match its indentation, quoting, and trailing commas exactly.

The `assets` variable type is fixed by `terraform/modules/chdb_aws/variables.tf:28-37`:

```hcl
assets = map(object({
  schema = list(object({
    name     = string
    type     = string
    required = optional(bool, false)
  }))
}))
```

Per-asset shape:

```hcl
<asset_name> = {
  schema = [
    { name = "<col>", type = "<iceberg_type>", required = true },
    { name = "<col>", type = "<iceberg_type>" },
  ]
}
```

## 4. Iceberg type reference

Primitives (use these verbatim as the `type` string):

- `boolean`
- `int` — 32-bit signed
- `long` — 64-bit signed (use for IDs)
- `float` — 32-bit
- `double` — 64-bit
- `decimal(P,S)` — exact; P ≤ 38, S ≤ P
- `date` — days since epoch
- `time` — microseconds of day
- `timestamp` — microseconds, no zone
- `timestamptz` — microseconds, UTC
- `string` — UTF-8, unbounded
- `uuid`
- `binary` — variable-length
- `fixed(L)` — fixed-length bytes

Nested types (write them as a single type string; the Terraform module passes the string through to the S3 Tables API):

- `list<elem>` — e.g. `list<string>`
- `map<key, value>` — keys must be primitive
- `struct<f1: t1, f2: t2>`

Avoid nested types unless you need them — chDB support is solid for primitives and gets thinner as nesting deepens.

## 5. PyArrow → Iceberg type mapping

The write path is `pq.read_table(...)` → `iceberg_writer.append(_cfg, asset, table)`. The PyArrow schema of the incoming parquet must align with the Iceberg table schema. Use this table when the input is a parquet file or a PyArrow schema:

| PyArrow | Iceberg | Notes |
|---|---|---|
| `pa.bool_()` | `boolean` | |
| `pa.int8()` / `pa.int16()` / `pa.int32()` | `int` | Widen to `long` if the domain can exceed 2³¹ |
| `pa.int64()` | `long` | Default for IDs |
| `pa.uint*()` | promote to the next signed type | Iceberg has no unsigned types |
| `pa.float32()` | `float` | |
| `pa.float64()` | `double` | |
| `pa.decimal128(p, s)` | `decimal(p, s)` | |
| `pa.string()` / `pa.large_string()` | `string` | |
| `pa.binary()` / `pa.large_binary()` | `binary` | Use `fixed(L)` only for genuinely fixed-width payloads |
| `pa.date32()` / `pa.date64()` | `date` | |
| `pa.time64('us')` | `time` | |
| `pa.timestamp('us')` no tz | `timestamp` | |
| `pa.timestamp('us', tz='UTC')` | `timestamptz` | Strongly preferred for event times |
| `pa.timestamp('ns', ...)` | downsample producer to `'us'` | Iceberg stores microseconds |
| `pa.list_(t)` / `pa.large_list(t)` | `list<t>` | |
| `pa.map_(k, v)` | `map<k, v>` | |
| `pa.struct([…])` | `struct<…>` | |
| `pa.dictionary(idx, value)` | map to the **value** type | The dictionary is a parquet encoding, not a logical type |

## 6. Naming conventions

- Asset key (map key) and every column `name` must match `^[a-z][a-z0-9_]*$`.
- The asset key becomes the S3 prefix via `src/chdb_aws/write/handler.py:16` (`^assets/(?P<asset>[^/]+)/dropzone/…`). No uppercase, slashes, or spaces.
- Avoid SQL reserved words (`select`, `from`, `where`, `order`, `group`, `timestamp`, `date`, `time`, `key`, `value`, `type`, `user`). Prefer `event_ts` over `timestamp`, `order_id` over `order`.
- Names are stable: Iceberg renames are legal but every chDB SQL template referring to the old name will break.

## 7. `required = true` — one-way door

`required = true` means strict `NOT NULL`. Iceberg *does* allow relaxing required → optional and tightening optional → required, but:

- Tightening optional → required is a **data migration**. Every existing row (including archived parquet) must already be non-null for the column, or the promotion fails.
- Relaxing required → optional is safe but consumers may assume non-null.

**Default to `required = false`.** Set `required = true` only for columns that are truly non-null in every record forever (primary keys, event type, event timestamp).

## 8. Timestamp handling

- Use `timestamptz` for anything that represents an instant in time (events, created_at, updated_at). Producer must emit `pa.timestamp('us', tz='UTC')`.
- Use `timestamp` only for wall-clock values that legitimately have no zone (e.g. a local-business-hours "open time").
- Iceberg stores microseconds. If your producer emits nanoseconds, downsample before writing parquet — don't change the schema to accommodate the producer.

## 9. Decimal, money, and ID rules

- **Money**: `decimal(18, 2)` for most currencies; `decimal(38, 4)` if you need more headroom or sub-cent precision. Never `float` / `double`.
- **Integer IDs**: `long`, always. A table that starts at `int` will eventually need a widening migration.
- **UUIDs**: use `uuid`. Producers typically write these as `pa.string()` in parquet — that's fine; both sides handle the conversion.
- **Hashes, tokens, opaque blobs**: `string` unless bytes are actually needed, then `binary`.

## 10. chDB compatibility notes

- `icebergS3()` in chDB ≥ 4.0 supports all primitives above, including `decimal`, `timestamptz`, and nested `list<>` / `struct<>`.
- chDB identifiers are case-sensitive inside `SELECT`. Use lowercase column names so SQL templates don't need quoting.
- The read path pins metadata per-query (`src/chdb_aws/read/query.py:34-42`) via `iceberg_metadata_file_path`, so clients always see the latest schema snapshot — no chDB-side schema cache to worry about.

## 11. Schema evolution rules (the lifecycle)

Classify every change to an **existing** asset before editing:

**Safe — no parquet rewrite, no downtime**

- Add an optional column
- Drop a column (column is tombstoned; parquet keeps the data, Iceberg hides it)
- Rename a column (but beware chDB SQL templates)
- Reorder columns
- Widen `int` → `long`
- Widen `float` → `double`
- Widen `decimal(P,S)` → `decimal(P', S)` where `P' > P` (scale must stay equal)

**Requires deliberate handling**

- Flip `required` (see §7)
- Change list/map element types — treat as a drop + add

**Unsafe / blocked**

- Narrow a type (`long` → `int`, `double` → `float`, shrinking decimal)
- Change type family (`string` ↔ `int`, `timestamp` ↔ `timestamptz`)
- Reuse a previously-dropped column name with a different type — Iceberg tracks by field ID and the reader may misinterpret old parquet. Use a new name instead.

**Effect on in-flight pipeline state** — always think about both prefixes:

- `assets/<asset>/archive/` — already-written parquet. Field-ID projection means Iceberg keeps reading it correctly under all safe changes above.
- `assets/<asset>/dropzone/` — unprocessed parquet. At the moment of `terraform apply` these files must match the new schema or the write Lambda will throw on `table.append(rows)`. **Drain the dropzone before a schema evolution**, or gate the evolution behind a producer code change that's already emitting the new shape.

## 12. Authoring workflow

Follow these steps every invocation:

1. **Scope and input**: one asset or many? natural description, parquet path, JSON sample, PyArrow schema, or diff against an existing asset?
2. **Derive per-asset schemas**: for parquet/JSON, build a PyArrow schema, then map each field via §5. For natural-language inputs, apply §8–§9 defaults.
3. **Apply naming rules** (§6) and **default `required = false`** except for obvious non-null columns (§7).
4. **For any asset that already exists in `terraform.tfvars`**, classify every change via §11. If any change is unsafe, **stop and surface it** — don't edit. If any change is destructive (asset deletion, required tightening), require explicit user confirmation.
5. **Read `terraform/main/terraform.tfvars`** and **edit in place** with the Edit tool:
   - New asset: insert `<name> = { schema = [ … ] }` inside the `assets = { … }` block, matching the indentation of the existing `events` entry
   - Modified asset: change only the fields that need changing
   - Removed asset: delete its entry (after confirmation)
   - Multi-asset requests: do them **all in one pass** — don't leave the file half-edited
6. **Report back**: (a) a diff summary, (b) the §13 validation checklist with pass/fail, (c) the command to preview: `cd terraform/main && terraform plan`, (d) any follow-ups — drain dropzone, update producer code, Lambda deploy.

## 13. Validation checklist

Run mentally before every edit. Every item must pass:

- [ ] Each asset key matches `^[a-z][a-z0-9_]*$`
- [ ] No asset key collides with an existing key (unless the user asked to modify that asset)
- [ ] Each column `name` matches the same pattern and is not a reserved word (§6)
- [ ] Each `type` is a valid primitive (§4) or a well-formed nested expression
- [ ] Every `required = true` column has a justification from the conversation
- [ ] For evolutions: every change is classified under §11 and none is "Unsafe / blocked"
- [ ] For destructive changes (delete asset, tighten `required`): user has explicitly confirmed
- [ ] HCL formatting matches the existing `events` entry (two-space indent, `=` alignment not required, trailing commas present)

## 14. Worked example — multiple tables in one invocation

**User:** *"Add `orders` (id, customer_id, amount in dollars, order_ts) and `shipments` (id, order_id, carrier, shipped_ts)."*

**Derived schemas** — IDs → `long`, money → `decimal(18,2)`, times → `timestamptz`:

```hcl
orders = {
  schema = [
    { name = "id", type = "long", required = true },
    { name = "customer_id", type = "long", required = true },
    { name = "amount", type = "decimal(18,2)" },
    { name = "order_ts", type = "timestamptz", required = true },
  ]
}

shipments = {
  schema = [
    { name = "id", type = "long", required = true },
    { name = "order_id", type = "long", required = true },
    { name = "carrier", type = "string" },
    { name = "shipped_ts", type = "timestamptz" },
  ]
}
```

**Resulting `terraform.tfvars` after the single edit:**

```hcl
assets = {
  events = {
    schema = [
      { name = "id", type = "string", required = true },
      { name = "event_type", type = "string" },
      { name = "ts", type = "timestamp" },
      { name = "payload", type = "string" },
    ]
  }

  orders = {
    schema = [
      { name = "id", type = "long", required = true },
      { name = "customer_id", type = "long", required = true },
      { name = "amount", type = "decimal(18,2)" },
      { name = "order_ts", type = "timestamptz", required = true },
    ]
  }

  shipments = {
    schema = [
      { name = "id", type = "long", required = true },
      { name = "order_id", type = "long", required = true },
      { name = "carrier", type = "string" },
      { name = "shipped_ts", type = "timestamptz" },
    ]
  }
}
```

Then: `cd terraform/main && terraform plan` → expect two new resources `aws_s3tables_table.asset["orders"]` and `aws_s3tables_table.asset["shipments"]`.

## 15. Worked example — parquet sample → single asset

Input: parquet with PyArrow schema

```
id: string
customer_id: int64
amount: decimal128(18, 2)
order_ts: timestamp[us, tz=UTC]
```

Mapping through §5 yields:

```hcl
orders = {
  schema = [
    { name = "id", type = "string", required = true },
    { name = "customer_id", type = "long", required = true },
    { name = "amount", type = "decimal(18,2)" },
    { name = "order_ts", type = "timestamptz", required = true },
  ]
}
```

After `terraform apply`, drop a matching parquet at `s3://<data-bucket>/assets/orders/dropzone/*.parquet` and invoke the read Lambda with `{"asset": "orders", "sql": "SELECT count() FROM ${asset}"}` to confirm the round-trip.

## 16. Evolution example — add a column safely

**User:** *"Add a nullable `user_agent` column to `events`."* This is a §11 safe change (add optional column).

Edit: insert `{ name = "user_agent", type = "string" }` into the `events.schema` list. Tell the user:

- No dropzone drain needed — existing parquet without the column will be read back with `NULL`s.
- Producers can start emitting `user_agent` at any time; older rows remain valid.
- Run `terraform plan` → `apply` → producers can begin populating the column.
