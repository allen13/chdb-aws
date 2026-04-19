#!/usr/bin/env python3
"""Performance Analysis — latency quantiles, slow paths, cache effectiveness.

Demonstrates chDB features: quantiles aggregate, conditional averages,
grouping by boolean, histogram-style quantile families across groups.
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
        "Latency quantiles — overall",
        """
        SELECT
            round(avg(response_time_ms), 2)            AS avg_ms,
            round(quantile(0.50)(response_time_ms), 1) AS p50_ms,
            round(quantile(0.90)(response_time_ms), 1) AS p90_ms,
            round(quantile(0.95)(response_time_ms), 1) AS p95_ms,
            round(quantile(0.99)(response_time_ms), 1) AS p99_ms,
            round(quantile(0.999)(response_time_ms), 1) AS p999_ms,
            max(response_time_ms)                      AS max_ms
        FROM ${asset}
        """,
    ),
    (
        "Slowest 10 paths (by p95, with ≥ 1k requests)",
        """
        SELECT
            path,
            count()                                    AS requests,
            round(quantile(0.50)(response_time_ms), 1) AS p50_ms,
            round(quantile(0.95)(response_time_ms), 1) AS p95_ms,
            round(quantile(0.99)(response_time_ms), 1) AS p99_ms
        FROM ${asset}
        GROUP BY path
        HAVING requests >= 1000
        ORDER BY p95_ms DESC
        LIMIT 10
        """,
    ),
    (
        "Cache effectiveness — hit vs. miss",
        """
        SELECT
            multiIf(cache_hit, 'HIT', 'MISS')          AS cache,
            count()                                    AS requests,
            round(count() * 100.0 / sum(count()) OVER (), 1) AS share_pct,
            round(avg(response_time_ms), 1)            AS avg_ms,
            round(quantile(0.95)(response_time_ms), 1) AS p95_ms,
            round(avg(bytes_sent))                     AS avg_bytes
        FROM ${asset}
        GROUP BY cache_hit
        ORDER BY requests DESC
        """,
    ),
    (
        "Response-size distribution by HTTP method",
        """
        SELECT
            http_method,
            count()                                 AS requests,
            round(avg(bytes_sent))                  AS avg_bytes,
            round(quantile(0.50)(bytes_sent))       AS p50_bytes,
            round(quantile(0.95)(bytes_sent))       AS p95_bytes,
            round(quantile(0.99)(bytes_sent))       AS p99_bytes,
            max(bytes_sent)                         AS max_bytes
        FROM ${asset}
        GROUP BY http_method
        ORDER BY requests DESC
        """,
    ),
]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--function-name", default=DEFAULT_FUNCTION)
    ap.add_argument("--asset", default=DEFAULT_ASSET)
    ap.add_argument("--region", default=DEFAULT_REGION)
    args = ap.parse_args()

    banner(
        "Performance Analysis",
        f"asset={args.asset}  ·  lambda={args.function_name}  ·  region={args.region}",
    )
    run_suite(args.function_name, args.asset, args.region, QUERIES)


if __name__ == "__main__":
    main()
