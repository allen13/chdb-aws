from functools import lru_cache

import pyarrow as pa
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


def append(cfg: Config, asset: str, rows: pa.Table) -> None:
    table = _catalog(cfg).load_table((cfg.namespace, asset))
    table.append(rows)
