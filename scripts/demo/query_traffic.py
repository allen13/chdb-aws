#!/usr/bin/env python3
"""Traffic Overview — volume, geography, time, and edge POP mix.

Demonstrates chDB features: approximate distinct counts, top-K, hourly
time-bucketing, window functions for share-of-total.
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
        "Snapshot — total volume, distinct clients, distinct paths, date range",
        """
        SELECT
            count()                AS requests,
            uniqExact(client_ip)   AS unique_clients,
            uniqExact(path)        AS unique_paths,
            min(ts)                AS first_seen,
            max(ts)                AS last_seen
        FROM ${asset}
        """,
    ),
    (
        "Top 10 countries by volume (with share-of-total via window fn)",
        """
        SELECT
            country,
            count() AS requests,
            round(count() * 100.0 / sum(count()) OVER (), 2) AS share_pct
        FROM ${asset}
        WHERE country != ''
        GROUP BY country
        ORDER BY requests DESC
        LIMIT 10
        """,
    ),
    (
        "Hourly volume (last 24 hours of data)",
        """
        SELECT
            toStartOfHour(ts) AS hour,
            count()           AS requests,
            uniqExact(client_ip) AS unique_clients
        FROM ${asset}
        WHERE ts >= (SELECT max(ts) FROM ${asset}) - INTERVAL 24 HOUR
        GROUP BY hour
        ORDER BY hour
        """,
    ),
    (
        "Top 10 edge POPs — volume, cache hit rate, avg latency",
        """
        SELECT
            edge_pop,
            count()                                       AS requests,
            round(avg(cache_hit) * 100, 1)                AS cache_hit_pct,
            round(avg(response_time_ms), 1)               AS avg_ms,
            round(quantile(0.95)(response_time_ms), 1)    AS p95_ms
        FROM ${asset}
        GROUP BY edge_pop
        ORDER BY requests DESC
        LIMIT 10
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
        "Traffic Overview",
        f"asset={args.asset}  ·  lambda={args.function_name}  ·  region={args.region}",
    )
    run_suite(args.function_name, args.asset, args.region, QUERIES)


if __name__ == "__main__":
    main()
