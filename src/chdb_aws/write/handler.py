import io
import logging
import re
from typing import Any
from urllib.parse import unquote_plus

import boto3
import pyarrow.parquet as pq

from chdb_aws.config import Config
from chdb_aws.write import archiver, iceberg_writer

log = logging.getLogger()
log.setLevel(logging.INFO)

_KEY_RE = re.compile(r"^assets/(?P<asset>[^/]+)/dropzone/[^/]+\.parquet$")

_cfg = Config.from_env()
_s3 = boto3.client("s3")


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    processed: list[str] = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        match = _KEY_RE.match(key)
        if not match:
            log.info("skipping non-dropzone key: %s", key)
            continue

        asset = match["asset"]
        log.info("processing s3://%s/%s into asset=%s", bucket, key, asset)

        body = _s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        table = pq.read_table(io.BytesIO(body))

        iceberg_writer.append(_cfg, asset, table)
        archiver.archive(_s3, bucket, key)
        processed.append(key)

    return {"processed": processed}
