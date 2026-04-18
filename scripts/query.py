#!/usr/bin/env python3
"""Invoke the chdb-aws read Lambda with a SQL query and print the body.

Example:
    uv run scripts/query.py \
        --function-name chdb-aws-dev-read \
        --asset test_orders \
        --sql 'SELECT sum(amount) FROM ${asset}'
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import boto3


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--function-name", required=True, help="read Lambda function name")
    ap.add_argument("--asset", required=True, help="asset name used in the SQL template")
    ap.add_argument(
        "--sql",
        required=True,
        help="SQL template; reference the table as ${asset}",
    )
    ap.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION", "us-east-1"),
        help="AWS region (default: $AWS_REGION or us-east-1)",
    )
    args = ap.parse_args()

    client = boto3.client("lambda", region_name=args.region)
    resp = client.invoke(
        FunctionName=args.function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps({"asset": args.asset, "sql": args.sql}).encode(),
    )

    raw = resp["Payload"].read().decode()
    if resp.get("FunctionError"):
        print(f"Lambda error: {raw}", file=sys.stderr)
        sys.exit(1)

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        print(f"unexpected Lambda response (not JSON): {raw}", file=sys.stderr)
        sys.exit(1)

    status = payload.get("statusCode")
    body = payload.get("body", "")
    if status != 200:
        print(f"statusCode={status}, body={body}", file=sys.stderr)
        sys.exit(1)

    print(body)


if __name__ == "__main__":
    main()
