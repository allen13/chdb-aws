#!/usr/bin/env python3
"""Generate a synthetic parquet file for a chdb-aws asset.

The asset's schema is read from a Terraform tfvars file so the generator,
terratest, and `terraform apply` all share a single source of truth.

Example:
    uv run scripts/generate_test_data.py \
        --tfvars terratest/dev/dev.tfvars \
        --asset test_orders \
        --rows 50 \
        --output /tmp/orders.parquet
"""
from __future__ import annotations

import argparse
import os
import random
import re
import sys
import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import hcl2
import pyarrow as pa
import pyarrow.parquet as pq


_DECIMAL_RE = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$")
_FIXED_RE = re.compile(r"^fixed\(\s*(\d+)\s*\)$")


def _unwrap(value: Any) -> Any:
    """python-hcl2 often wraps top-level values in single-element lists and preserves
    literal quotes around string values (treating them as templates). Normalize both."""
    while isinstance(value, list) and len(value) == 1 and isinstance(value[0], (dict, list)):
        value = value[0]
    if isinstance(value, str) and len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return value[1:-1]
    if isinstance(value, dict):
        return {k: _unwrap(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_unwrap(v) for v in value]
    return value


def _load_schema(tfvars_path: Path, asset: str) -> list[dict[str, Any]]:
    with tfvars_path.open() as f:
        parsed = hcl2.load(f)

    assets = _unwrap(parsed.get("assets"))
    if not isinstance(assets, dict) or asset not in assets:
        available = sorted(assets.keys()) if isinstance(assets, dict) else []
        raise SystemExit(
            f"asset {asset!r} not found in {tfvars_path}; "
            f"available: {available}"
        )

    entry = assets[asset]
    schema = entry.get("schema") if isinstance(entry, dict) else None
    if not isinstance(schema, list):
        raise SystemExit(f"asset {asset!r} has no schema list in {tfvars_path}")

    return schema


def _iceberg_to_pyarrow(iceberg_type: str) -> pa.DataType:
    t = iceberg_type.strip().lower()
    simple = {
        "boolean": pa.bool_(),
        "int": pa.int32(),
        "long": pa.int64(),
        "float": pa.float32(),
        "double": pa.float64(),
        "date": pa.date32(),
        "time": pa.time64("us"),
        "timestamp": pa.timestamp("us"),
        "timestamptz": pa.timestamp("us", tz="UTC"),
        "string": pa.string(),
        "uuid": pa.string(),
        "binary": pa.binary(),
    }
    if t in simple:
        return simple[t]

    m = _DECIMAL_RE.match(t)
    if m:
        return pa.decimal128(int(m.group(1)), int(m.group(2)))

    m = _FIXED_RE.match(t)
    if m:
        return pa.binary(int(m.group(1)))

    raise SystemExit(
        f"unsupported Iceberg type {iceberg_type!r} "
        f"(generator covers primitives + decimal + fixed only)"
    )


def _is_id_column(name: str) -> bool:
    return name == "id" or name.endswith("_id")


def _gen_value(field_name: str, dtype: pa.DataType, row_idx: int) -> Any:
    if pa.types.is_boolean(dtype):
        return random.random() < 0.5
    if pa.types.is_integer(dtype):
        if _is_id_column(field_name):
            return row_idx + 1
        return random.randint(0, 1_000_000)
    if pa.types.is_floating(dtype):
        return random.uniform(0, 10_000)
    if pa.types.is_decimal(dtype):
        scale = dtype.scale
        precision = dtype.precision
        max_whole = 10 ** min(precision - scale, 12) - 1
        whole = random.randint(0, max_whole)
        frac = random.randint(0, 10**scale - 1)
        return Decimal(f"{whole}.{frac:0{scale}d}")
    if pa.types.is_date(dtype):
        return date.today() - timedelta(days=random.randint(0, 365))
    if pa.types.is_time(dtype):
        return random.randint(0, 24 * 3600 * 1_000_000 - 1)
    if pa.types.is_timestamp(dtype):
        base = datetime.now(tz=UTC)
        offset = timedelta(microseconds=row_idx)
        ts = base - offset
        return ts if dtype.tz else ts.replace(tzinfo=None)
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        if field_name.lower() == "email":
            return f"user{row_idx}@example.test"
        if field_name.lower() == "currency":
            return random.choice(["USD", "EUR", "GBP", "JPY"])
        return uuid.uuid4().hex
    if pa.types.is_fixed_size_binary(dtype):
        return os.urandom(dtype.byte_width)
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        return os.urandom(16)
    raise SystemExit(f"no value generator for PyArrow type {dtype}")


def _build_table(schema_fields: list[dict[str, Any]], rows: int) -> pa.Table:
    pa_fields: list[pa.Field] = []
    columns: dict[str, list[Any]] = {}

    for field in schema_fields:
        name = field["name"]
        iceberg_type = field["type"]
        required = bool(field.get("required", False))
        dtype = _iceberg_to_pyarrow(iceberg_type)
        pa_fields.append(pa.field(name, dtype, nullable=not required))
        columns[name] = [_gen_value(name, dtype, i) for i in range(rows)]

    pa_schema = pa.schema(pa_fields)
    arrays = [pa.array(columns[f.name], type=f.type) for f in pa_schema]
    return pa.Table.from_arrays(arrays, schema=pa_schema)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tfvars", type=Path, required=True, help="path to a .tfvars file")
    ap.add_argument("--asset", required=True, help="asset name (key in the assets map)")
    ap.add_argument("--rows", type=int, default=50, help="number of rows (default 50)")
    ap.add_argument("--output", type=Path, required=True, help="output parquet path")
    ap.add_argument("--seed", type=int, default=None, help="optional RNG seed")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    schema_fields = _load_schema(args.tfvars, args.asset)
    table = _build_table(schema_fields, args.rows)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, args.output)
    print(
        f"wrote {args.rows} rows for asset={args.asset} -> {args.output}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
