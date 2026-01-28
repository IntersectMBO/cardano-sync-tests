import logging
import pathlib as pl
import subprocess
from pathlib import Path

from sync_tests.utils import helpers
from sync_tests.utils.db_sync_config import DbSyncConfig

LOGGER = logging.getLogger(__name__)


def export_epoch_sync_times_from_db(
    config: DbSyncConfig, file: str | Path, snapshot_epoch_no: int | str = 0
) -> str | None:
    """Export epoch synchronization times from the database to a file.

    Args:
        config: A DbSyncConfig instance with database connection settings.
        file: Path to the output file.
        snapshot_epoch_no: Minimum epoch number to export (defaults to 0).

    Returns:
        str: Output from psql command, or None on error.
    """
    repo_root = pl.Path(__file__).parent.parent.parent
    db_sync_dir = repo_root / "cardano-db-sync"
    output_file = pl.Path(file).resolve() if not isinstance(file, Path) else file.resolve()
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
                f"Error during exporting epoch sync times from db: {err}. "
                "Killing extraction process."
            )
            p.kill()
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        p.kill()
        LOGGER.exception(
            "Error during exporting epoch sync times from db. Killing extraction process."
        )
    except Exception:
        LOGGER.exception("Error during exporting epoch sync times from db. Killing extraction process.")
        p.kill()
    else:
        return out

    return None

