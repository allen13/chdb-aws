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
    backend = event.get("backend", "s3tables")
    engine = event.get("engine", "materialize")
    columns = event.get("columns")  # list[str] or None — only used by engine=scan
    where = event.get("where")  # str expression or None — only used by engine=scan
    log.info(
        "running query against asset=%s backend=%s engine=%s",
        asset,
        backend,
        engine,
    )

    body = query.query(
        _cfg,
        asset,
        sql,
        backend=backend,
        engine=engine,
        columns=columns,
        where=where,
    )
    return {"statusCode": 200, "body": body}
