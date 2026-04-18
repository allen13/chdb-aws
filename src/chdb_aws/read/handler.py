import logging
from typing import Any

from chdb_aws.config import Config
from chdb_aws.read import query

log = logging.getLogger()
log.setLevel(logging.INFO)

_cfg = Config.from_env()


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    asset = event["asset"]
    sql = event["sql"]
    log.info("running query against asset=%s", asset)

    body = query.query(_cfg, asset, sql)
    return {"statusCode": 200, "body": body}
