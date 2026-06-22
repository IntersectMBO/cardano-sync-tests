"""Database query helpers for exporting epoch sync times from DB Sync."""

from __future__ import annotations

import logging
import pathlib as pl
import subprocess

from sync_tests.utils.db_sync.config import DbSyncConfig

LOGGER = logging.getLogger(__name__)


def export_epoch_sync_times_from_db(
    config: DbSyncConfig, file: str | pl.Path, snapshot_epoch_no: int | str = 0
) -> str | None:
    """Export epoch synchronization times from the database to a file.

    Args:
        config: A DbSyncConfig instance with database connection settings.
        file: Path to the output file.
        snapshot_epoch_no: Minimum epoch number to export (defaults to 0).

    Returns:
        str: Output from psql command, or None on error.
    """
    repo_root = pl.Path(__file__).parent.parent.parent.parent
    db_sync_dir = repo_root / "cardano-db-sync"
    output_file = pl.Path(file).resolve() if not isinstance(file, pl.Path) else file.resolve()
    p: subprocess.Popen[bytes] | None = None
    try:
        p = subprocess.Popen(
            [
                "psql",
                config.pg_dbname,
                "-t",
                "-c",
                rf"\o {output_file}",
                "-c",
                "SELECT array_to_json(array_agg(epoch_sync_time), FALSE) FROM "
                f"epoch_sync_time where no >= {snapshot_epoch_no};",
            ],
            cwd=str(db_sync_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        outs, errs = p.communicate(timeout=600)
        out = outs.decode("utf-8").strip() if outs else ""
        err = errs.decode("utf-8").strip() if errs else ""
        if err:
            LOGGER.error(
                "Error during exporting epoch sync times from db: %s. Killing extraction process.",
                err,
            )
            p.kill()
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        if p is not None:
            p.kill()
        LOGGER.exception(
            "Error during exporting epoch sync times from db. Killing extraction process."
        )
    except Exception:
        LOGGER.exception(
            "Error during exporting epoch sync times from db. Killing extraction process."
        )
        if p is not None:
            p.kill()
    else:
        return out

    return None
