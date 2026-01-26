"""Smoke check to ensure logs are written to test_workdir."""

from __future__ import annotations

import pathlib as pl

from sync_tests.utils import db_sync


def main() -> None:
    root_dir = pl.Path.cwd()
    test_workdir = root_dir / "test_workdir"
    test_workdir.mkdir(exist_ok=True)

    config = db_sync.create_db_sync_config(env="preview", workdir=test_workdir)

    expected_node_log = test_workdir / "logfile.log"
    expected_db_sync_log = test_workdir / "db_sync_preview_logfile.log"

    if config.node_log_file != expected_node_log:
        raise SystemExit(
            f"node_log_file mismatch: expected {expected_node_log}, got {config.node_log_file}"
        )
    if config.db_sync_log_file != expected_db_sync_log:
        raise SystemExit(
            f"db_sync_log_file mismatch: expected {expected_db_sync_log}, got {config.db_sync_log_file}"
        )

    print("Log path smoke check passed.")


if __name__ == "__main__":
    main()

