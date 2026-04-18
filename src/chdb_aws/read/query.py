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


def query(cfg: Config, asset: str, sql_template: str) -> str:
    """Run a chDB SQL query against the asset's S3 Tables table.

    chDB's native ``icebergS3()`` talks to the underlying bucket with plain S3
    API, which S3 Tables refuses. We use pyiceberg (which goes through the
    S3 Tables REST catalog + vended credentials) to materialize the table as a
    local parquet file, then let chDB read that via ``file()``.
    """
    table = _catalog(cfg).load_table((cfg.namespace, asset))
    arrow_table = table.scan().to_arrow()

    fd, path = tempfile.mkstemp(suffix=".parquet", dir="/tmp")
    os.close(fd)
    try:
        pq.write_table(arrow_table, path)
        table_fn = f"file('{path}', 'Parquet')"
        sql = sql_template.replace("${asset}", table_fn)
        return str(chdb.query(sql, cfg.result_format))
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
