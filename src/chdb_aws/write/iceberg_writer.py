"""Write the same Arrow batch to both Iceberg backends.

* ``s3tables`` — AWS S3 Tables via its Iceberg REST endpoint. Storage lives
  in AWS-managed ``*--table-s3`` buckets that refuse plain S3 listing, so
  chDB's native ``icebergS3()`` can't read them directly.

* ``glue`` — regular S3 bucket + AWS Glue Data Catalog. Same Iceberg format,
  but the storage is in our own bucket so chDB's ``icebergS3()`` works fine.

Both writes happen on every append so the two backends stay in lock-step.
On first write the Glue table is created using the s3tables table's schema
as the source of truth — preserves ``timestamptz`` and ``required`` flags
that the Glue control plane would otherwise drop.

A failure on either backend raises and the source parquet stays in dropzone
for the Lambda to retry on the next event.
"""
from __future__ import annotations

import logging
from functools import lru_cache

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError

from chdb_aws.config import Config

log = logging.getLogger()


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


def _ensure_glue_table(cfg: Config, asset: str, source_schema):
    """Load (or create) the Glue Iceberg table mirroring the s3tables schema."""
    catalog = _glue_catalog(cfg)
    ident = (cfg.glue_database, asset)
    try:
        return catalog.load_table(ident)
    except NoSuchTableError:
        log.info("creating glue iceberg table %s.%s", cfg.glue_database, asset)
        return catalog.create_table(
            identifier=ident,
            schema=source_schema,
            location=f"s3://{cfg.iceberg_bucket}/{asset}/",
        )


def append(cfg: Config, asset: str, rows: pa.Table) -> None:
    """Append ``rows`` to the asset in both backends."""
    s3t = _s3tables_catalog(cfg).load_table((cfg.namespace, asset))
    s3t.append(rows)
    log.info("appended %d rows to s3tables.%s.%s", len(rows), cfg.namespace, asset)

    glue = _ensure_glue_table(cfg, asset, source_schema=s3t.schema())
    glue.append(rows)
    log.info("appended %d rows to glue.%s.%s", len(rows), cfg.glue_database, asset)
