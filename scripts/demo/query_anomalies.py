#!/usr/bin/env python3
"""Error & Anomaly Detection — 5xx trends, offender IPs, slow outliers, bots.

Demonstrates chDB features: conditional aggregates (countIf), nested
subqueries for p99 thresholds, regex matching (match), CASE-style multiIf.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _helpers import (  # noqa: E402
    DEFAULT_ASSET,
    DEFAULT_FUNCTION,
    DEFAULT_REGION,
    banner,
    run_suite,
)


QUERIES = [
    (
        "Error rate over time (15-minute buckets, last 6h of data)",
        """
        SELECT
            toStartOfInterval(ts, INTERVAL 15 MINUTE) AS bucket,
            count()                                    AS requests,
            countIf(status_code >= 500)                AS errors_5xx,
            countIf(status_code BETWEEN 400 AND 499)   AS errors_4xx,
            round(countIf(status_code >= 500) * 100.0 / count(), 3) AS error_rate_pct
        FROM ${asset}
        WHERE ts >= (SELECT max(ts) FROM ${asset}) - INTERVAL 6 HOUR
        GROUP BY bucket
        ORDER BY bucket DESC
        LIMIT 24
        """,
    ),
    (
        "Top 10 offender IPs by 4xx/5xx count",
        """
        SELECT
            client_ip,
            count()                        AS requests,
            countIf(status_code >= 500)    AS errors_5xx,
            countIf(status_code BETWEEN 400 AND 499) AS errors_4xx,
            round(countIf(status_code >= 400) * 100.0 / count(), 1) AS err_pct,
            uniqExact(path)                AS distinct_paths
        FROM ${asset}
        GROUP BY client_ip
        HAVING errors_5xx + errors_4xx >= 1
        ORDER BY errors_5xx DESC, errors_4xx DESC
        LIMIT 10
        """,
    ),
    (
        "Slow outliers — requests above the overall p99 latency",
        """
        WITH (SELECT quantile(0.99)(response_time_ms) FROM ${asset}) AS p99_ms
        SELECT
            ts,
            edge_pop,
            country,
            http_method,
            path,
            status_code,
            response_time_ms,
            bytes_sent,
            cache_hit
        FROM ${asset}
        WHERE response_time_ms > p99_ms
        ORDER BY response_time_ms DESC
        LIMIT 15
        """,
    ),
    (
        "Bot vs. human traffic (regex on user-agent)",
        """
        SELECT
            multiIf(
                match(user_agent, '(?i)bot|crawler|scanner|spider'),  'BOT',
                match(user_agent, '(?i)curl|Go-http|python-requests|PostmanRuntime|okhttp'), 'CLIENT-LIB',
                'BROWSER'
            )                                          AS segment,
            count()                                    AS requests,
            round(count() * 100.0 / sum(count()) OVER (), 1) AS share_pct,
            round(avg(response_time_ms), 1)            AS avg_ms,
            round(quantile(0.95)(response_time_ms), 1) AS p95_ms,
            round(avg(cache_hit) * 100, 1)             AS cache_hit_pct
        FROM ${asset}
        GROUP BY segment
        ORDER BY requests DESC
        """,
    ),
]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--function-name", default=DEFAULT_FUNCTION)
    ap.add_argument("--asset", default=DEFAULT_ASSET)
    ap.add_argument("--region", default=DEFAULT_REGION)
    ap.add_argument("--backend", choices=("s3tables", "glue"), default="glue")
    ap.add_argument(
        "--engine",
        choices=("materialize", "scan", "iceberg_s3"),
        default="iceberg_s3",
    )
    args = ap.parse_args()

    banner(
        "Error & Anomaly Detection",
        f"asset={args.asset}  ·  backend={args.backend}  ·  engine={args.engine}",
    )
    run_suite(
        args.function_name, args.asset, args.region, QUERIES,
        backend=args.backend, engine=args.engine,
    )


if __name__ == "__main__":
    main()
