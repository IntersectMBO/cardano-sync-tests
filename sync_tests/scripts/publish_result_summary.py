#!/usr/bin/env python3
"""CLI script to publish sync test result metrics to the CI job summary.

Reads node_sync_results.json and/or db_sync_results.json from a workdir and
renders their key metrics as a markdown block. Writes to GITHUB_STEP_SUMMARY
when set (GitHub Actions), otherwise prints to stdout for local use.

Purely informational: never raises and never affects the caller's exit code,
a failure here must not be mistaken for a test failure.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import pathlib as pl
import typing as tp

LOGGER = logging.getLogger(__name__)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish sync result metrics to the CI job summary."
    )
    parser.add_argument(
        "-w", "--workdir", required=True, help="Directory containing result JSON files."
    )
    return parser.parse_args()


def _format_bytes(num_bytes: tp.Any) -> str:
    """Render a byte count as a human-readable size, or "unknown" if absent."""
    if not isinstance(num_bytes, int | float) or num_bytes <= 0:
        return "unknown"
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


def render_node_summary(data: dict[str, tp.Any]) -> list[str]:
    """Render key metrics from a node_sync_results.json payload as markdown lines."""
    eras = data.get("eras_in_test") or []
    return [
        "### Node sync",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Environment | {data.get('env', 'unknown')} |",
        f"| Node revision | {data.get('tag_no1', 'unknown')} |",
        f"| Sync time | {data.get('sync_time1', 'unknown')} |",
        f"| Chain size | {_format_bytes(data.get('chain_size_bytes'))} |",
        f"| Eras reached | {', '.join(eras) if eras else 'unknown'} |",
        "",
    ]


def render_dbsync_summary(data: dict[str, tp.Any]) -> list[str]:
    """Render key metrics from a db_sync_results.json payload as markdown lines."""
    epoch = data.get("last_synced_epoch_no", "unknown")
    block = data.get("last_synced_block_no", "unknown")
    return [
        "### DB-sync",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Environment | {data.get('env', 'unknown')} |",
        f"| DB-sync revision | {data.get('db_sync_revision', 'unknown')} |",
        f"| Sync time | {data.get('total_sync_time_in_h_m_s', 'unknown')} |",
        f"| Database size | {data.get('total_database_size', 'unknown')} |",
        f"| Last synced | epoch {epoch}, block {block} |",
        f"| Rollbacks detected | {'Yes' if data.get('rollbacks') else 'No'} |",
        f"| Errors detected | {'Yes' if data.get('errors') else 'No'} |",
        "",
    ]


def _load_json(path: pl.Path) -> dict[str, tp.Any] | None:
    try:
        with open(path, encoding="utf-8") as fh:
            return tp.cast("dict[str, tp.Any]", json.load(fh))
    except Exception:
        LOGGER.warning("Failed to read %s; skipping its summary section.", path, exc_info=True)
        return None


def build_summary_lines(workdir: pl.Path) -> list[str]:
    """Build the full markdown summary from whatever result files exist in workdir."""
    lines: list[str] = []

    node_data = _load_json(workdir / "node_sync_results.json")
    if node_data is not None:
        lines.extend(render_node_summary(node_data))

    dbsync_data = _load_json(workdir / "db_sync_results.json")
    if dbsync_data is not None:
        lines.extend(render_dbsync_summary(dbsync_data))

    if lines:
        lines = ["## Sync results", "", *lines]
    return lines


def publish(lines: list[str]) -> None:
    """Write the summary to GITHUB_STEP_SUMMARY when set, else print it."""
    if not lines:
        LOGGER.info("No sync result files found; nothing to publish.")
        return

    output = "\n".join(lines)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as fh:
            fh.write(output + "\n")
    else:
        print(output)


def main() -> int:
    args = get_args()
    try:
        lines = build_summary_lines(pl.Path(args.workdir))
        publish(lines)
    except Exception:
        # Purely informational script: never fail the caller over a reporting glitch.
        LOGGER.warning("Failed to publish sync result summary", exc_info=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
