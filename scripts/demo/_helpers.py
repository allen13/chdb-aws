"""Shared helpers for the demo scripts under scripts/demo/.

Keeps Lambda invocation, result parsing, and rich-based rendering in one
place so each query script stays short and declarative.
"""
from __future__ import annotations

import functools
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from rich.align import Align
from rich.columns import Columns
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text


_REPO_ROOT = Path(__file__).resolve().parents[2]
_MAIN_TF_DIR = _REPO_ROOT / "terraform" / "main"


@functools.lru_cache(maxsize=1)
def _terraform_outputs() -> dict[str, Any]:
    """Read `terraform output -json` from terraform/main/ once. Returns {} on
    any failure (no terraform binary, no state, etc.) so callers can fall
    back to env vars / hardcoded defaults."""
    if shutil.which("terraform") is None or not _MAIN_TF_DIR.exists():
        return {}
    try:
        proc = subprocess.run(
            ["terraform", f"-chdir={_MAIN_TF_DIR}", "output", "-json"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (subprocess.SubprocessError, OSError):
        return {}
    if proc.returncode != 0 or not proc.stdout.strip():
        return {}
    try:
        raw = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return {}
    return {k: v.get("value") for k, v in raw.items() if isinstance(v, dict)}


def _resolve(env_var: str, tf_key: str, fallback: str) -> str:
    """Env var > terraform output > hardcoded fallback."""
    if (val := os.environ.get(env_var)):
        return val
    if (val := _terraform_outputs().get(tf_key)):
        return str(val)
    return fallback


DEFAULT_REGION = os.environ.get("AWS_REGION", "us-east-1")
DEFAULT_FUNCTION = _resolve("CHDB_READ_FUNCTION", "read_lambda_function_name", "chdb-aws-prod-read")
DEFAULT_DATA_BUCKET = _resolve("CHDB_DATA_BUCKET", "data_bucket_name", "chdb-aws-prod-data")
DEFAULT_ASSET = os.environ.get("CHDB_ASSET", "requests")


console = Console()


@dataclass
class QueryResult:
    """Parsed result + timing for a single read-Lambda invocation."""

    meta: list[dict[str, str]]  # [{"name": "...", "type": "..."}, ...]
    rows: list[list[Any]]
    row_count: int
    chdb_elapsed_s: float
    chdb_rows_read: int
    chdb_bytes_read: int
    wall_elapsed_s: float
    lambda_billed_ms: int | None


def _lambda_client(region: str):
    return boto3.client("lambda", region_name=region)


def invoke_read(
    function_name: str,
    asset: str,
    sql: str,
    region: str = DEFAULT_REGION,
    *,
    backend: str = "glue",
    engine: str = "iceberg_s3",
    columns: list[str] | None = None,
    where: str | None = None,
) -> QueryResult:
    client = _lambda_client(region)
    payload: dict[str, Any] = {
        "asset": asset,
        "sql": sql,
        "backend": backend,
        "engine": engine,
    }
    if columns:
        payload["columns"] = list(columns)
    if where:
        payload["where"] = where
    start = time.perf_counter()
    resp = client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode(),
    )
    wall = time.perf_counter() - start

    raw = resp["Payload"].read().decode()
    if resp.get("FunctionError"):
        raise RuntimeError(f"Lambda error: {raw}")

    payload = json.loads(raw)
    status = payload.get("statusCode")
    body = payload.get("body", "")
    if status != 200:
        raise RuntimeError(f"Lambda returned statusCode={status}: {body}")

    parsed = json.loads(body)
    stats = parsed.get("statistics", {})
    return QueryResult(
        meta=parsed.get("meta", []),
        rows=parsed.get("data", []),
        row_count=parsed.get("rows", len(parsed.get("data", []))),
        chdb_elapsed_s=float(stats.get("elapsed", 0.0)),
        chdb_rows_read=int(stats.get("rows_read", 0)),
        chdb_bytes_read=int(stats.get("bytes_read", 0)),
        wall_elapsed_s=wall,
        lambda_billed_ms=None,
    )


def _fmt_scalar(value: Any, col_type: str) -> str:
    if value is None:
        return "—"
    t = col_type.lower()
    if "date" in t or "time" in t:
        return str(value)
    if "decimal" in t or "float" in t or "double" in t:
        try:
            return f"{float(value):,.2f}"
        except (TypeError, ValueError):
            return str(value)
    if "int" in t:
        try:
            return f"{int(value):,}"
        except (TypeError, ValueError):
            return str(value)
    return str(value)


def _fmt_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    units = ["KiB", "MiB", "GiB", "TiB"]
    size = float(n)
    for u in units:
        size /= 1024
        if size < 1024:
            return f"{size:,.1f} {u}"
    return f"{size:,.1f} PiB"


def render_query(title: str, sql: str, result: QueryResult) -> None:
    """Pretty-print a single query: title banner, SQL, result table, metrics."""
    console.print()
    console.print(
        Panel.fit(
            Text(title, style="bold white"),
            border_style="cyan",
            padding=(0, 2),
        )
    )

    sql_block = Syntax(
        sql.strip(),
        "sql",
        theme="monokai",
        background_color="default",
        word_wrap=True,
    )
    console.print(sql_block)
    console.print()

    if not result.meta:
        console.print("[yellow]no columns returned[/]")
    else:
        table = Table(
            show_header=True,
            header_style="bold cyan",
            border_style="dim",
            pad_edge=False,
        )
        for col in result.meta:
            justify = "right" if _is_numeric(col.get("type", "")) else "left"
            table.add_column(col["name"], justify=justify)
        for row in result.rows:
            table.add_row(
                *(_fmt_scalar(v, c.get("type", "")) for v, c in zip(row, result.meta))
            )
        console.print(table)

    metrics = Columns(
        [
            Text.assemble(("wall ", "dim"), (f"{result.wall_elapsed_s * 1000:,.0f} ms", "bold green")),
            Text.assemble(("chdb ", "dim"), (f"{result.chdb_elapsed_s * 1000:,.1f} ms", "bold magenta")),
            Text.assemble(("rows read ", "dim"), (f"{result.chdb_rows_read:,}", "bold yellow")),
            Text.assemble(("bytes read ", "dim"), (_fmt_bytes(result.chdb_bytes_read), "bold yellow")),
            Text.assemble(("returned ", "dim"), (f"{result.row_count:,} rows", "bold")),
        ],
        expand=False,
        padding=(0, 2),
    )
    console.print(metrics)


def _is_numeric(col_type: str) -> bool:
    t = col_type.lower()
    return any(k in t for k in ("int", "float", "double", "decimal"))


def banner(title: str, subtitle: str = "") -> None:
    text = Text()
    text.append(title, style="bold cyan")
    if subtitle:
        text.append("\n")
        text.append(subtitle, style="dim")
    console.print()
    console.print(
        Panel(
            Align.center(text),
            border_style="bright_cyan",
            padding=(1, 4),
        )
    )
    console.print()


def summarize(total_wall_s: float, query_count: int, total_rows_read: int, total_bytes_read: int) -> None:
    """Final footer after all queries in a script."""
    summary = Text()
    summary.append(f"{query_count} queries", style="bold")
    summary.append(" | wall ", style="dim")
    summary.append(f"{total_wall_s:,.2f} s", style="bold green")
    summary.append(" | rows read ", style="dim")
    summary.append(f"{total_rows_read:,}", style="bold yellow")
    summary.append(" | bytes read ", style="dim")
    summary.append(_fmt_bytes(total_bytes_read), style="bold yellow")
    console.print()
    console.print(Panel(Align.center(summary), border_style="bright_cyan", padding=(0, 2)))
    console.print()


def run_suite(
    function_name: str,
    asset: str,
    region: str,
    queries: list[tuple[str, str]],
    *,
    backend: str = "glue",
    engine: str = "iceberg_s3",
) -> None:
    """Run a list of (title, sql) pairs, rendering each and a final summary."""
    total_wall = 0.0
    total_rows = 0
    total_bytes = 0
    for title, sql in queries:
        try:
            result = invoke_read(
                function_name, asset, sql, region=region,
                backend=backend, engine=engine,
            )
        except Exception as exc:  # noqa: BLE001
            console.print(f"[red]error running {title!r}: {exc}[/red]")
            sys.exit(1)
        render_query(title, sql, result)
        total_wall += result.wall_elapsed_s
        total_rows += result.chdb_rows_read
        total_bytes += result.chdb_bytes_read
    summarize(total_wall, len(queries), total_rows, total_bytes)
