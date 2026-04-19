#!/usr/bin/env python3
"""Invoke the chdb-aws read Lambda with a SQL query and print the body.

Examples:
    # default engine — full snapshot materialized + cached on /tmp
    uv run scripts/query.py \\
        --function-name chdb-aws-prod-read \\
        --asset requests \\
        --sql 'SELECT count() FROM ${asset}'

    # scan engine — pyiceberg pushes column projection + predicate
    uv run scripts/query.py \\
        --function-name chdb-aws-prod-read \\
        --asset requests \\
        --sql 'SELECT count() FROM ${asset}' \\
        --engine scan \\
        --column status_code \\
        --where 'status_code >= 500'

    # iceberg_s3 engine — chDB native; experimental, expected to 405
    uv run scripts/query.py \\
        --function-name chdb-aws-prod-read \\
        --asset requests \\
        --sql 'SELECT count() FROM ${asset}' \\
        --engine iceberg_s3
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import boto3


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--function-name", required=True, help="read Lambda function name")
    ap.add_argument("--asset", required=True, help="asset name used in the SQL template")
    ap.add_argument(
        "--sql",
        required=True,
        help="SQL template; reference the table as ${asset}",
    )
    ap.add_argument(
        "--backend",
        choices=("s3tables", "glue"),
        default="s3tables",
        help="iceberg backend to read from (default: s3tables)",
    )
    ap.add_argument(
        "--engine",
        choices=("materialize", "scan", "iceberg_s3"),
        default="materialize",
        help="read engine (default: materialize)",
    )
    ap.add_argument(
        "--column",
        action="append",
        dest="columns",
        metavar="NAME",
        help="repeatable; columns to project. Only used by --engine scan.",
    )
    ap.add_argument(
        "--where",
        help="row filter expression (pyiceberg syntax). Only used by --engine scan.",
    )
    ap.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION", "us-east-1"),
        help="AWS region (default: $AWS_REGION or us-east-1)",
    )
    args = ap.parse_args()

    payload: dict = {
        "asset": args.asset,
        "sql": args.sql,
        "backend": args.backend,
        "engine": args.engine,
    }
    if args.columns:
        payload["columns"] = args.columns
    if args.where:
        payload["where"] = args.where

    client = boto3.client("lambda", region_name=args.region)
    resp = client.invoke(
        FunctionName=args.function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode(),
    )

    raw = resp["Payload"].read().decode()
    if resp.get("FunctionError"):
        print(f"Lambda error: {raw}", file=sys.stderr)
        sys.exit(1)

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        print(f"unexpected Lambda response (not JSON): {raw}", file=sys.stderr)
        sys.exit(1)

    status = parsed.get("statusCode")
    body = parsed.get("body", "")
    if status != 200:
        print(f"statusCode={status}, body={body}", file=sys.stderr)
        sys.exit(1)

    print(body)


if __name__ == "__main__":
    main()
