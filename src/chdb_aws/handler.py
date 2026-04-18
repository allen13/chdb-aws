from typing import Any


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    return {"statusCode": 200, "body": "ok"}
