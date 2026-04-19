import hashlib
import os
import tempfile
from functools import lru_cache

import chdb  # pyright: ignore[reportMissingImports]
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog, load_catalog

from chdb_aws.config import Config


@lru_cache(maxsize=1)
def _catalog(cfg: Config) -> Catalog:
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


def _cache_path(namespace: str, asset: str, metadata_location: str) -> str:
    key = hashlib.sha1(
        f"{namespace}/{asset}/{metadata_location}".encode()
    ).hexdigest()[:16]
    return f"/tmp/chdb_cache_{key}.parquet"


def _materialize(table, path: str) -> None:
    fd, tmp = tempfile.mkstemp(suffix=".parquet", dir="/tmp")
    os.close(fd)
    try:
        pq.write_table(table.scan().to_arrow(), tmp)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except FileNotFoundError:
            pass
        raise


def query(cfg: Config, asset: str, sql_template: str) -> str:
    """Run a chDB SQL query against the asset's S3 Tables table.

    chDB's native ``icebergS3()`` uses plain S3 API which S3 Tables refuses,
    so we use pyiceberg (via the S3 Tables REST catalog + vended creds) to
    materialize the table to a parquet file under ``/tmp`` and let chDB read
    it via ``file()``. The parquet file is cached across warm Lambda
    invocations, keyed on the table's current metadata location — a new
    snapshot (new write) yields a new cache key, so stale data can't be read.
    """
    table = _catalog(cfg).load_table((cfg.namespace, asset))
    path = _cache_path(cfg.namespace, asset, table.metadata_location)
    if not os.path.exists(path):
        _materialize(table, path)

    table_fn = f"file('{path}', 'Parquet')"
    sql = sql_template.replace("${asset}", table_fn)
    return str(chdb.query(sql, cfg.result_format))
