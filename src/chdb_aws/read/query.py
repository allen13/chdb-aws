"""Read-side query engines for chdb-aws.

Two orthogonal axes:

* **Backend** — where the Iceberg metadata + data files live.
  - ``s3tables`` — AWS S3 Tables (default; the original storage backend).
  - ``glue`` — AWS Glue Data Catalog + a regular S3 bucket. Same data,
    written in parallel by the write Lambda. This backend's metadata + data
    sit in a normal S3 bucket so chDB's native ``icebergS3()`` can read it.

* **Engine** — how the read Lambda gets the data into chDB.
  - ``materialize`` (default) — pyiceberg full-snapshot scan → ``/tmp``
    parquet, cached by ``(backend, namespace, asset, metadata_location)``.
    Subsequent queries against the same snapshot re-use the cache.
  - ``scan`` — pyiceberg ``Table.scan(selected_fields=…, row_filter=…)`` so
    column projection + (manifest-level) predicate pruning happen before
    any parquet write. Per-call temp file, no cache.
  - ``iceberg_s3`` — chDB's native ``icebergS3()`` table function pointed at
    the latest ``metadata.json`` URL. Works with backend=``glue`` (regular
    S3 bucket); fails with backend=``s3tables`` because the underlying
    ``*--table-s3`` bucket policy denies ``GetBucketLocation``.

The read-Lambda event payload looks like:

    {
        "asset": "requests",
        "sql":   "SELECT count() FROM ${asset}",
        "backend": "glue" | "s3tables" (default "s3tables"),
        "engine":  "materialize" | "scan" | "iceberg_s3" (default "materialize"),
        "columns": ["status_code", ...]   # only used by engine=scan
        "where":   "status_code >= 500"   # only used by engine=scan
    }
"""
from __future__ import annotations

import hashlib
import logging
import os
import tempfile
from functools import lru_cache
from typing import Any, Iterable

import boto3
import chdb  # pyright: ignore[reportMissingImports]
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog, load_catalog

from chdb_aws.config import Config

log = logging.getLogger()
_s3tables = boto3.client("s3tables")


# ----------------------------------------------------------------------------
# Catalogs (one per backend, lru_cached)
# ----------------------------------------------------------------------------


@lru_cache(maxsize=1)
def _s3tables_catalog(cfg: Config) -> Catalog:
    return load_catalog(
        "s3tables",
        **{
            "type": "rest",
            "warehouse": cfg.table_bucket_arn,
            "uri": f"https://s3tables.{cfg.region}.amazonaws.com/iceberg",
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "s3tables",
            "rest.signing-region": cfg.region,
        },
    )


@lru_cache(maxsize=1)
def _glue_catalog(cfg: Config) -> Catalog:
    return load_catalog(
        "glue",
        **{
            "type": "glue",
            "warehouse": f"s3://{cfg.iceberg_bucket}/",
            "glue.region": cfg.region,
        },
    )


def _load_table(cfg: Config, asset: str, backend: str):
    if backend == "s3tables":
        return _s3tables_catalog(cfg).load_table((cfg.namespace, asset))
    if backend == "glue":
        return _glue_catalog(cfg).load_table((cfg.glue_database, asset))
    raise ValueError(f"unknown backend: {backend!r} (expected s3tables or glue)")


# ----------------------------------------------------------------------------
# Public dispatcher
# ----------------------------------------------------------------------------


def query(
    cfg: Config,
    asset: str,
    sql_template: str,
    *,
    backend: str = "s3tables",
    engine: str = "materialize",
    columns: Iterable[str] | None = None,
    where: str | None = None,
) -> str:
    if engine == "materialize":
        return _query_via_materialize(cfg, asset, sql_template, backend=backend)
    if engine == "scan":
        return _query_via_scan(
            cfg, asset, sql_template, backend=backend, columns=columns, where=where
        )
    if engine == "iceberg_s3":
        return _query_via_iceberg_s3(cfg, asset, sql_template, backend=backend)
    raise ValueError(
        f"unknown engine: {engine!r} (expected materialize | scan | iceberg_s3)"
    )


# ----------------------------------------------------------------------------
# Engine 1 — full materialize, snapshot-cached
# ----------------------------------------------------------------------------


def _cache_path(backend: str, namespace: str, asset: str, metadata_location: str) -> str:
    key = hashlib.sha1(
        f"{backend}/{namespace}/{asset}/{metadata_location}".encode()
    ).hexdigest()[:16]
    return f"/tmp/chdb_cache_{key}.parquet"


def _atomic_write_parquet(arrow_table, dst_path: str) -> None:
    fd, tmp = tempfile.mkstemp(suffix=".parquet", dir="/tmp")
    os.close(fd)
    try:
        pq.write_table(arrow_table, tmp)
        os.replace(tmp, dst_path)
    except BaseException:
        try:
            os.remove(tmp)
        except FileNotFoundError:
            pass
        raise


def _query_via_materialize(cfg: Config, asset: str, sql_template: str, *, backend: str) -> str:
    table = _load_table(cfg, asset, backend)
    namespace = cfg.namespace if backend == "s3tables" else cfg.glue_database
    path = _cache_path(backend, namespace, asset, table.metadata_location)
    if not os.path.exists(path):
        _atomic_write_parquet(table.scan().to_arrow(), path)
    return _run_chdb_against_file(cfg, sql_template, path)


# ----------------------------------------------------------------------------
# Engine 2 — pyiceberg scan with pushdown, per-call (no cache)
# ----------------------------------------------------------------------------


def _query_via_scan(
    cfg: Config,
    asset: str,
    sql_template: str,
    *,
    backend: str,
    columns: Iterable[str] | None,
    where: str | None,
) -> str:
    table = _load_table(cfg, asset, backend)

    scan_kwargs: dict[str, Any] = {}
    if columns:
        scan_kwargs["selected_fields"] = tuple(columns)
    if where:
        scan_kwargs["row_filter"] = where  # pyiceberg parses string expressions
    arrow = table.scan(**scan_kwargs).to_arrow()

    fd, path = tempfile.mkstemp(suffix=".parquet", dir="/tmp")
    os.close(fd)
    try:
        pq.write_table(arrow, path)
        return _run_chdb_against_file(cfg, sql_template, path)
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


# ----------------------------------------------------------------------------
# Engine 3 — chDB native icebergS3()
# ----------------------------------------------------------------------------


def _query_via_iceberg_s3(cfg: Config, asset: str, sql_template: str, *, backend: str) -> str:
    if backend == "s3tables":
        # The metadata file IS readable via plain GetObject, but ClickHouse's
        # S3 client calls GetBucketLocation on the underlying *--table-s3
        # bucket to discover the region — and that operation is denied by the
        # bucket policy regardless of caller IAM. There's no known way to
        # disable that from the icebergS3 call site at chDB 26.1.x. We surface
        # a clear error rather than letting chDB report the cryptic 405.
        raise RuntimeError(
            "iceberg_s3 + backend=s3tables is unsupported: AWS S3 Tables' "
            "underlying bucket policy denies GetBucketLocation, which "
            "ClickHouse calls before any read. Use backend=glue for "
            "icebergS3, or backend=s3tables with engine=materialize/scan."
        )

    table = _load_table(cfg, asset, backend)
    # chDB's icebergS3() takes the *table root*, not the metadata file URL.
    # It auto-discovers the latest snapshot by listing the metadata/
    # directory — works fine on a regular S3 bucket where ListObjectsV2 is
    # allowed.
    table_root = table.location().rstrip("/") + "/"
    table_fn = f"icebergS3('{table_root}')"
    sql = sql_template.replace("${asset}", table_fn)
    return str(chdb.query(sql, cfg.result_format))


# ----------------------------------------------------------------------------
# Shared helper
# ----------------------------------------------------------------------------


def _run_chdb_against_file(cfg: Config, sql_template: str, path: str) -> str:
    table_fn = f"file('{path}', 'Parquet')"
    sql = sql_template.replace("${asset}", table_fn)
    return str(chdb.query(sql, cfg.result_format))
