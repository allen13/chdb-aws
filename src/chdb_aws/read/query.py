from urllib.parse import urlparse

import boto3
import chdb  # pyright: ignore[reportMissingImports]

from chdb_aws.config import Config

_s3tables = boto3.client("s3tables")


def _split_metadata_uri(uri: str) -> tuple[str, str]:
    """s3://bucket/path/to/table/metadata/00001-x.metadata.json
       -> ("s3://bucket/path/to/table/", "metadata/00001-x.metadata.json")"""
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"unexpected metadata URI: {uri!r}")
    key = parsed.path.lstrip("/")
    sep = "/metadata/"
    idx = key.rfind(sep)
    if idx < 0:
        raise ValueError(f"no /metadata/ segment in {uri!r}")
    table_root = f"s3://{parsed.netloc}/{key[: idx + 1]}"
    relative_metadata = key[idx + 1 :]
    return table_root, relative_metadata


def query(cfg: Config, asset: str, sql_template: str) -> str:
    """Run a chDB SQL query against the asset's S3 Tables table.

    sql_template references the table as ``${asset}`` — it is replaced with
    a fully-qualified ``icebergS3(...)`` table function bound to the current
    metadata snapshot retrieved from the s3tables API.
    """
    location = _s3tables.get_table_metadata_location(
        tableBucketARN=cfg.table_bucket_arn,
        namespace=cfg.namespace,
        name=asset,
    )["metadataLocation"]

    table_root, rel_metadata = _split_metadata_uri(location)
    table_fn = f"icebergS3('{table_root}')"
    settings = f"SETTINGS iceberg_metadata_file_path = '{rel_metadata}'"

    sql = f"{sql_template.replace('${asset}', table_fn)} {settings}"
    return str(chdb.query(sql, cfg.result_format))
