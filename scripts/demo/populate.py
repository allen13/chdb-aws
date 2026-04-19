#!/usr/bin/env python3
"""Populate the `requests` asset with realistic synthetic CDN edge-log data.

Generates N rows split into B parquet batches, uploads each batch to the
dropzone prefix (which the write Lambda listens on), and polls for the
archive marker so the progress bar reflects real ingestion.

Example:
    uv run scripts/demo/populate.py --rows 1000000 --batches 10
"""
from __future__ import annotations

import argparse
import os
import random
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import boto3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _helpers import (  # noqa: E402
    DEFAULT_ASSET,
    DEFAULT_DATA_BUCKET,
    DEFAULT_REGION,
    banner,
    console,
)

EDGE_POPS = [
    "iad71", "iad72", "sfo5", "sfo9", "lax3", "ord51", "atl56",
    "sea19", "bos50", "dfw55", "ewr52", "mia51", "fra6", "ams4",
    "lhr61", "lhr62", "cdg50", "nrt57", "hnd51", "sin4", "syd4",
    "gru3", "scl2", "yyz50", "mex2", "bom2", "del1", "hkg60",
    "icn55", "jnb1",
]

COUNTRIES = [
    "US", "CA", "GB", "DE", "FR", "NL", "IE", "IT", "ES", "SE",
    "PL", "BR", "MX", "AR", "JP", "KR", "SG", "AU", "IN", "ZA",
    "AE", "IL", "TR", "CL", "CH",
]

HTTP_METHODS = (
    ("GET", 0.80),
    ("POST", 0.14),
    ("PUT", 0.03),
    ("DELETE", 0.02),
    ("PATCH", 0.01),
)

PATHS = [
    "/", "/health", "/api/v1/products", "/api/v1/products/{id}",
    "/api/v1/cart", "/api/v1/cart/items", "/api/v1/checkout",
    "/api/v1/search", "/api/v1/search/suggestions", "/api/v1/users/me",
    "/api/v1/orders", "/api/v1/orders/{id}", "/api/v1/orders/{id}/status",
    "/api/v1/recommendations", "/api/v1/reviews", "/api/v1/reviews/{id}",
    "/api/v1/categories", "/api/v1/categories/{slug}",
    "/api/v1/inventory", "/api/v1/inventory/{sku}",
    "/assets/app.js", "/assets/app.css", "/assets/logo.svg",
    "/assets/hero.jpg", "/assets/bundle.min.js",
    "/images/product-{n}.webp", "/images/banner-{n}.jpg",
    "/static/fonts/inter.woff2", "/static/fonts/mono.woff2",
    "/robots.txt", "/sitemap.xml", "/favicon.ico",
    "/admin/metrics", "/admin/healthcheck", "/admin/config",
    "/api/v2/graphql", "/api/v2/stream", "/api/v2/feed",
    "/webhooks/stripe", "/webhooks/segment", "/webhooks/slack",
]

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit Chrome/122",
    "Mozilla/5.0 (Windows NT 10.0) AppleWebKit Chrome/121",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3) AppleWebKit Safari",
    "Mozilla/5.0 (iPad; CPU OS 17_1) Safari",
    "Mozilla/5.0 (Linux; Android 14) Chrome/120 Mobile",
    "Mozilla/5.0 (X11; Linux x86_64) Firefox/124",
    "Mozilla/5.0 compatible; Googlebot/2.1",
    "Mozilla/5.0 compatible; bingbot/2.0",
    "curl/8.4.0",
    "Go-http-client/1.1",
    "python-requests/2.31",
    "okhttp/4.12",
    "PostmanRuntime/7.36",
    "AhrefsBot/7.0; scanner",
    "SemrushBot/7~bl; crawler",
]

REFERRERS = [
    "",
    "https://www.google.com/",
    "https://duckduckgo.com/",
    "https://twitter.com/",
    "https://news.ycombinator.com/",
    "https://github.com/",
    "https://reddit.com/r/programming",
    "https://linkedin.com/feed",
]

STATUS_CODES = (
    (200, 0.88),
    (204, 0.03),
    (301, 0.02),
    (304, 0.03),
    (400, 0.01),
    (401, 0.005),
    (403, 0.005),
    (404, 0.015),
    (429, 0.003),
    (500, 0.004),
    (502, 0.003),
    (503, 0.002),
)


def _weighted_choices(rng: np.random.Generator, pairs, size: int):
    values, weights = zip(*pairs, strict=True)
    total = sum(weights)
    probs = [w / total for w in weights]
    return rng.choice(values, size=size, p=probs)


def _random_ips(rng: np.random.Generator, size: int) -> np.ndarray:
    # 5 "frequent" IPs + a long tail, gives interesting top-N shapes
    heavy = np.array([
        "203.0.113.7", "198.51.100.42", "192.0.2.55",
        "203.0.113.199", "198.51.100.88",
    ])
    heavy_rate = 0.15
    mask = rng.random(size) < heavy_rate
    out = np.empty(size, dtype=object)
    n_heavy = int(mask.sum())
    out[mask] = rng.choice(heavy, size=n_heavy)
    n_rand = size - n_heavy
    rand_octets = rng.integers(1, 254, size=(n_rand, 4))
    out[~mask] = [".".join(map(str, row)) for row in rand_octets]
    return out


def build_batch(rng: np.random.Generator, size: int, ts_start: datetime) -> pa.Table:
    request_ids = np.array([uuid.uuid4().hex for _ in range(size)])

    # Spread timestamps uniformly across the 7-day window, sorted ascending.
    window_s = 7 * 24 * 3600
    offsets = np.sort(rng.random(size)) * window_s
    ts = [ts_start + timedelta(seconds=float(s)) for s in offsets]

    edge_pop = rng.choice(EDGE_POPS, size=size, p=_softmax_pop_weights())
    country = rng.choice(COUNTRIES, size=size, p=_softmax_country_weights())
    method = _weighted_choices(rng, HTTP_METHODS, size)
    path = rng.choice(PATHS, size=size)
    status_code = _weighted_choices(rng, STATUS_CODES, size).astype(np.int32)

    # Lognormal-ish latency with a long tail; 90% cache-hit rate.
    cache_hit = rng.random(size) < 0.90
    latency_base = rng.lognormal(mean=2.5, sigma=0.7, size=size)
    latency_hit_penalty = rng.lognormal(mean=4.2, sigma=1.1, size=size) * 3
    response_time_ms = np.where(cache_hit, latency_base, latency_hit_penalty).astype(np.int32)
    response_time_ms = np.clip(response_time_ms, 1, 60000)

    # Bytes correlated with path type (assets are bigger).
    asset_mask = np.array(["/assets/" in p or "/images/" in p or "/static/" in p for p in path])
    bytes_sent = np.where(
        asset_mask,
        rng.lognormal(mean=10.5, sigma=0.6, size=size),
        rng.lognormal(mean=7.5, sigma=0.9, size=size),
    ).astype(np.int64)

    user_agent = rng.choice(USER_AGENTS, size=size)
    referrer = rng.choice(REFERRERS, size=size)
    client_ip = _random_ips(rng, size)

    schema = pa.schema([
        pa.field("request_id", pa.string(), nullable=False),
        pa.field("ts", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("edge_pop", pa.string()),
        pa.field("client_ip", pa.string()),
        pa.field("country", pa.string()),
        pa.field("http_method", pa.string()),
        pa.field("path", pa.string()),
        pa.field("status_code", pa.int32()),
        pa.field("bytes_sent", pa.int64()),
        pa.field("response_time_ms", pa.int32()),
        pa.field("cache_hit", pa.bool_()),
        pa.field("user_agent", pa.string()),
        pa.field("referrer", pa.string()),
    ])

    return pa.Table.from_arrays(
        [
            pa.array(request_ids, type=pa.string()),
            pa.array(ts, type=pa.timestamp("us", tz="UTC")),
            pa.array(edge_pop, type=pa.string()),
            pa.array(client_ip, type=pa.string()),
            pa.array(country, type=pa.string()),
            pa.array(method, type=pa.string()),
            pa.array(path, type=pa.string()),
            pa.array(status_code, type=pa.int32()),
            pa.array(bytes_sent, type=pa.int64()),
            pa.array(response_time_ms, type=pa.int32()),
            pa.array(cache_hit, type=pa.bool_()),
            pa.array(user_agent, type=pa.string()),
            pa.array(referrer, type=pa.string()),
        ],
        schema=schema,
    )


def _softmax_pop_weights() -> list[float]:
    # A handful of POPs take the lion's share; rest are long tail.
    weights = [8, 7, 6, 5, 5, 4, 4, 4, 3, 3] + [2] * 10 + [1] * 10
    total = sum(weights)
    return [w / total for w in weights]


def _softmax_country_weights() -> list[float]:
    weights = [10, 5, 4, 4, 3, 3, 3, 2, 2, 2] + [1.5] * 8 + [0.8] * 7
    total = sum(weights)
    return [w / total for w in weights]


def _wait_for_archive(
    s3: "boto3.client",
    bucket: str,
    dropzone_key: str,
    archive_key: str,
    timeout_s: float = 180.0,
    tick_s: float = 2.0,
) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if _head_ok(s3, bucket, archive_key) and not _head_ok(s3, bucket, dropzone_key):
            return True
        time.sleep(tick_s)
    return False


def _head_ok(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rows", type=int, default=1_000_000, help="total rows across all batches (default 1,000,000)")
    ap.add_argument("--batches", type=int, default=10, help="number of parquet files to upload (default 10)")
    ap.add_argument("--asset", default=DEFAULT_ASSET)
    ap.add_argument("--data-bucket", default=DEFAULT_DATA_BUCKET)
    ap.add_argument("--region", default=DEFAULT_REGION)
    ap.add_argument("--seed", type=int, default=0xC0FFEE)
    ap.add_argument("--archive-timeout", type=float, default=240.0, help="per-batch archive wait (s)")
    ap.add_argument(
        "--no-wait",
        action="store_true",
        help="upload all files then return; skip waiting for archive markers",
    )
    args = ap.parse_args()

    if args.batches < 1:
        ap.error("--batches must be >= 1")
    if args.rows < args.batches:
        ap.error("--rows must be >= --batches")

    rows_per_batch = args.rows // args.batches
    remainder = args.rows - rows_per_batch * args.batches
    batch_sizes = [rows_per_batch + (1 if i < remainder else 0) for i in range(args.batches)]

    banner(
        f"populating → s3://{args.data_bucket}/assets/{args.asset}/",
        f"{args.rows:,} rows across {args.batches} parquet batches  ·  region={args.region}",
    )

    rng = np.random.default_rng(args.seed)
    random.seed(args.seed)
    s3 = boto3.client("s3", region_name=args.region)

    ts_start = datetime.now(tz=UTC) - timedelta(days=7)
    start = time.perf_counter()
    bytes_uploaded = 0
    archived = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TextColumn("[dim]{task.fields[info]}"),
        console=console,
    ) as progress:
        task = progress.add_task(
            "uploading batches",
            total=args.batches,
            info="",
        )
        for i, n in enumerate(batch_sizes):
            table = build_batch(rng, n, ts_start)
            fname = f"batch-{int(time.time()*1000)}-{i:03d}-{uuid.uuid4().hex[:8]}.parquet"
            dz_key = f"assets/{args.asset}/dropzone/{fname}"
            ar_key = f"assets/{args.asset}/archive/{fname}"

            tmp_path = f"/tmp/{fname}"
            pq.write_table(table, tmp_path, compression="zstd")
            size = os.path.getsize(tmp_path)
            s3.upload_file(tmp_path, args.data_bucket, dz_key)
            os.remove(tmp_path)
            bytes_uploaded += size

            if args.no_wait:
                progress.update(
                    task,
                    advance=1,
                    info=f"uploaded batch {i+1} ({n:,} rows, {size / 1e6:.1f} MB)",
                )
                continue

            ok = _wait_for_archive(
                s3, args.data_bucket, dz_key, ar_key, timeout_s=args.archive_timeout
            )
            if not ok:
                progress.stop()
                console.print(
                    f"[red]timed out waiting for archive of {dz_key}; aborting[/red]"
                )
                sys.exit(2)
            archived += 1
            progress.update(
                task,
                advance=1,
                info=f"archived batch {i+1}/{args.batches}",
            )

    elapsed = time.perf_counter() - start
    mb = bytes_uploaded / (1024 * 1024)
    rows_per_s = args.rows / max(elapsed, 1e-9)
    mb_per_s = mb / max(elapsed, 1e-9)

    banner(
        "ingestion complete",
        (
            f"{args.rows:,} rows  ·  {mb:,.1f} MB uploaded  ·  {elapsed:,.1f} s  ·  "
            f"{rows_per_s:,.0f} rows/s  ·  {mb_per_s:,.2f} MB/s"
        ),
    )


if __name__ == "__main__":
    main()
