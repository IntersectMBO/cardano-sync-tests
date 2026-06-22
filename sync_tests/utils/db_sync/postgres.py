"""PostgreSQL setup, management, and DB Sync progress queries."""

from __future__ import annotations

import logging
import os
import pathlib as pl
import signal
import subprocess
import time
import typing as tp

import psycopg2

from sync_tests.utils import helpers
from sync_tests.utils.db_sync.config import DbSyncConfig
from sync_tests.utils.db_sync.config import DbSyncTip

LOGGER = logging.getLogger(__name__)

ONE_MINUTE = 60


def setup_postgres(config: DbSyncConfig, pg_port: str | None = None) -> None:
    """Set up PostgreSQL for use with Cardano DB Sync.

    Args:
        config: A DbSyncConfig instance with PostgreSQL settings.
        pg_port: Optional PostgreSQL port override (defaults to config.pg_port).
    """
    postgres_port = pg_port if pg_port is not None else config.pg_port
    helpers.export_env_var("POSTGRES_DIR", str(config.pg_dir))
    helpers.export_env_var("PGHOST", config.pg_host)
    helpers.export_env_var("PGUSER", config.pg_user)
    helpers.export_env_var("PGPORT", postgres_port)

    repo_root = pl.Path(__file__).parent.parent.parent.parent
    script_path = repo_root / "sync_tests" / "scripts" / "postgres-start.sh"
    try:
        cmd = [str(script_path), str(config.pg_dir), "-k"]
        # Run from repo root so the shebang `nix develop .#postgres` finds flake.nix
        # immediately (avoids misleading "test_workdir does not contain flake.nix" in logs).
        output = (
            subprocess.check_output(cmd, cwd=str(repo_root), stderr=subprocess.STDOUT)
            .decode("utf-8")
            .strip()
        )
        LOGGER.info("Setup postgres script output: %s", output)
    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e

    # Start monitor once per workspace; skip if already running, re-start if stale.
    start_monitor(config.workdir, config.env)


def start_monitor(workdir: pl.Path, env: str) -> None:
    """Start the background resource monitor for this workspace.

    Launches ``monitor-start.sh`` as a detached process, writing its PID to
    ``<workdir>/monitor.pid``.  If the PID file already points to a live
    process the call is a no-op.  Stale PID files (dead process) are removed
    and the monitor is re-launched.

    Args:
        workdir: The test working directory where logs and the PID file are written.
        env: Environment name used to label the log file (e.g. ``mainnet``).
    """
    repo_root = pl.Path(__file__).parent.parent.parent.parent
    monitor_pid_file = workdir / "monitor.pid"
    _monitor_running = False
    if monitor_pid_file.exists():
        try:
            _pid = int(monitor_pid_file.read_text().strip())
            os.kill(_pid, 0)  # raises OSError if process is dead
            _monitor_running = True
        except (OSError, ValueError):
            monitor_pid_file.unlink(missing_ok=True)

    if not _monitor_running:
        monitor_script = repo_root / "sync_tests" / "scripts" / "monitor-start.sh"
        monitor_stderr = workdir / "monitor_stderr.log"
        try:
            ferr = monitor_stderr.open("a", encoding="utf-8", errors="replace")
            try:
                monitor_proc = subprocess.Popen(
                    ["bash", str(monitor_script), str(workdir), env],
                    stdout=subprocess.DEVNULL,
                    stderr=ferr,
                )
                monitor_pid_file.write_text(str(monitor_proc.pid))
            finally:
                ferr.close()
            LOGGER.info(
                "Started resource monitor (PID %s) → %s/monitor.log",
                monitor_proc.pid,
                workdir,
            )
        except OSError:
            LOGGER.warning("Failed to start resource monitor", exc_info=True)
    else:
        LOGGER.info("Resource monitor already running (PID file exists: %s)", monitor_pid_file)


def stop_monitor(workdir: pl.Path) -> None:
    """Stop the background resource monitor started for this workspace.

    Reads the PID from ``<workdir>/monitor.pid``, sends SIGTERM, and removes
    the PID file so subsequent runs can start a fresh monitor.

    Args:
        workdir: The test working directory containing ``monitor.pid``.
    """
    pid_file = workdir / "monitor.pid"
    if not pid_file.exists():
        return
    try:
        pid = int(pid_file.read_text().strip())
        os.kill(pid, signal.SIGTERM)
        LOGGER.info("Stopped resource monitor (PID %s)", pid)
    except (ProcessLookupError, ValueError):
        LOGGER.debug("monitor.pid contained stale/invalid PID; monitor already stopped")
    except OSError:
        LOGGER.warning("Failed to stop resource monitor", exc_info=True)
    finally:
        pid_file.unlink(missing_ok=True)


def stop_postgres(config: DbSyncConfig) -> None:
    """Stop the PostgreSQL instance that was started for this test workspace.

    Reads the PID from ``<pg_dir>/data/postmaster.pid`` (written by postgres on
    startup) and sends SIGTERM to that specific process.  Falls back to a
    ``pg_ctl stop`` if the PID file is absent.  Never uses a broad process-name
    kill so host/shared postgres instances are never affected.

    Args:
        config: A DbSyncConfig instance whose ``pg_dir`` points to the postgres
            data directory used by this workspace.
    """
    data_dir = config.pg_dir / "data"
    pid_file = data_dir / "postmaster.pid"

    if pid_file.exists():
        try:
            pid = int(pid_file.read_text().splitlines()[0].strip())
            LOGGER.info("Stopping workspace postgres (PID %s) via SIGTERM", pid)
            os.kill(pid, signal.SIGTERM)
            # Wait up to 30 s for graceful shutdown.
            for _ in range(30):
                try:
                    os.kill(pid, 0)  # probe: raises OSError when process is gone
                    time.sleep(1)
                except OSError:
                    break
        except (ProcessLookupError, ValueError):
            LOGGER.warning(
                "postmaster.pid contained stale/invalid PID; postgres may already be stopped",
            )
            return
        except Exception:
            LOGGER.exception("Unexpected error stopping postgres via postmaster.pid; trying pg_ctl")
        else:
            LOGGER.info("Workspace postgres stopped")
            return

    # Fallback: pg_ctl (may not be on PATH in all environments)
    try:
        result = subprocess.run(
            ["pg_ctl", "stop", "-D", str(data_dir), "-m", "fast"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if result.returncode == 0:
            LOGGER.info("Workspace postgres stopped via pg_ctl")
        else:
            LOGGER.warning("pg_ctl stop returned %s: %s", result.returncode, result.stderr.strip())
    except FileNotFoundError:
        LOGGER.warning("pg_ctl not found on PATH; postgres was not explicitly stopped")
    except Exception:
        LOGGER.exception("Failed to stop postgres via pg_ctl")


def create_pgpass_file(config: DbSyncConfig) -> None:
    """Create a PostgreSQL password file for the specified environment.

    Args:
        config: A DbSyncConfig instance with paths and PostgreSQL settings.
    """
    repo_root = pl.Path(__file__).parent.parent.parent.parent
    db_sync_config_dir = repo_root / "cardano-db-sync" / "config"
    db_sync_config_dir.mkdir(parents=True, exist_ok=True)

    pgpass_file = db_sync_config_dir / f"pgpass-{config.env}"
    postgres_port = os.getenv("PGPORT", config.pg_port)
    # Use PG host (TCP), not pg_dir. pg_dir was historically used as the Unix socket
    # directory; socket runtime may live under /tmp/syncpg-* when pg_dir paths are long,
    # so pgpass must match PGHOST (localhost) rather than an on-disk postgres folder.
    pg_host = os.getenv("PGHOST", config.pg_host)
    pgpass_content = f"{pg_host}:{postgres_port}:{config.pg_dbname}:{config.pg_user}:*"
    helpers.export_env_var("PGPASSFILE", str(pgpass_file))

    with open(pgpass_file, "w") as pgpass_text_file:
        print(pgpass_content, file=pgpass_text_file)
    os.chmod(pgpass_file, 0o600)


def create_database() -> None:
    """Set up the PostgreSQL database for use with Cardano DB Sync."""
    repo_root = pl.Path(__file__).parent.parent.parent.parent
    db_sync_dir = repo_root / "cardano-db-sync"
    script_path = db_sync_dir / "scripts" / "postgresql-setup.sh"

    try:
        cmd = [str(script_path), "--createdb"]
        output = (
            subprocess.check_output(cmd, cwd=str(db_sync_dir), stderr=subprocess.STDOUT)
            .decode("utf-8")
            .strip()
        )
        LOGGER.info("Create database script output: %s", output)
    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e
    if "All good!" not in output:
        msg = "Create database has not ended successfully"
        raise RuntimeError(msg)


def list_databases(config: DbSyncConfig) -> None:
    """List all databases available in the PostgreSQL instance.

    Args:
        config: A DbSyncConfig instance with PostgreSQL settings.
    """
    cmd = ["psql", "-U", config.pg_user, "-l"]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")

    try:
        outs, errs = p.communicate(timeout=60)
        LOGGER.info("List databases: %s", outs)
        if errs:
            LOGGER.error("Error in list databases: %s", errs)
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        p.kill()
        raise


def get_db_sync_tip(config: DbSyncConfig) -> DbSyncTip | None:
    """Retrieve the tip information from the Cardano DB Sync database.

    Args:
        config: A DbSyncConfig instance with database connection settings.

    Returns:
        DbSyncTip: Tip information with epoch, block, and slot numbers.
        None: If tip data cannot be retrieved after retries.
    """
    cmd = [
        "psql",
        "-P",
        "pager=off",
        "-qt",
        "-U",
        config.pg_user,
        "-d",
        config.pg_dbname,
        "-c",
        "select epoch_no, block_no, slot_no from block order by id desc limit 1;",
    ]

    max_attempts = 6
    for attempt in range(1, max_attempts + 1):
        output_string = ""
        err_msg = ""
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=180,
                check=False,
            )
            output_string = result.stdout.strip()
            err_msg = result.stderr.strip()

            # Check for PostgreSQL connection errors
            if err_msg and ("connection" in err_msg.lower() or "fatal" in err_msg.lower()):
                msg = f"PostgreSQL connection error: {err_msg}"
                raise RuntimeError(msg)

            # Query may return empty while db-sync is still initializing.
            if not output_string:
                if attempt == max_attempts:
                    LOGGER.warning(
                        "No blocks found in database yet - db-sync may not have started syncing"
                    )
                    return None
                LOGGER.debug("No blocks in database yet (attempt %s/%s), waiting...", attempt, 6)
                time.sleep(ONE_MINUTE)
                continue

            # Parse the query result (format: epoch_no | block_no | slot_no)
            parts = [e.strip() for e in output_string.split("|")]
            if len(parts) != 3 or not all(parts):
                msg = f"Unexpected query result format: {output_string}"
                raise ValueError(msg)

            epoch_no_str, block_no_str, slot_no_str = parts
            return DbSyncTip(
                epoch_no=int(epoch_no_str),
                block_no=int(block_no_str),
                slot_no=int(slot_no_str),
            )
        except (subprocess.TimeoutExpired, RuntimeError, ValueError) as e:
            if attempt == max_attempts:
                msg = (
                    "Failed to get db-sync tip after multiple retries. "
                    f"Last output: '{output_string}', stderr: '{err_msg}'"
                )
                raise RuntimeError(msg) from e

            LOGGER.warning(
                "db-sync tip data unavailable (attempt %s/%s). "
                "Output from psql: '%s', errs: '%s'. Retrying...",
                attempt,
                max_attempts,
                output_string,
                err_msg or str(e),
            )
            time.sleep(ONE_MINUTE)

    return None


def get_db_sync_progress(config: DbSyncConfig) -> float | None:
    """Calculate the synchronization progress of the Cardano DB Sync database.

    Args:
        config: A DbSyncConfig instance with database connection settings.

    Returns:
        float: Sync progress percentage, or None if unavailable.
    """
    progress_query = (
        "select 100 * (extract (epoch from (max (time) at time zone 'UTC')) "
        "- extract (epoch from (min (time) at time zone 'UTC'))) "
        "/ (extract (epoch from (now () at time zone 'UTC')) "
        "- extract (epoch from (min (time) at time zone 'UTC'))) as sync_percent from block ;"
    )

    p = subprocess.Popen(
        [
            "psql",
            "-P",
            "pager=off",
            "-qt",
            "-U",
            config.pg_user,
            "-d",
            config.pg_dbname,
            "-c",
            progress_query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    should_try = True
    counter = 0

    while should_try:
        progress_string = ""
        try:
            outs, _errs = p.communicate(timeout=300)
            progress_string = outs.decode("utf-8").strip()
            # Handle empty string (db-sync hasn't started syncing yet)
            if not progress_string or progress_string.isspace():
                if counter > 5:
                    LOGGER.warning(
                        "No sync progress available - db-sync may not have started syncing yet"
                    )
                    return None
                # Not an error yet - db-sync might just be starting up
                LOGGER.debug(
                    "No sync progress available yet (attempt %s/6), waiting...",
                    counter + 1,
                )
                counter += 1
                time.sleep(ONE_MINUTE)
                # Create new subprocess for next attempt
                p = subprocess.Popen(
                    [
                        "psql",
                        "-P",
                        "pager=off",
                        "-qt",
                        "-U",
                        config.pg_user,
                        "-d",
                        config.pg_dbname,
                        "-c",
                        progress_query,
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                continue
            db_sync_progress = round(float(progress_string), 2)
        except Exception:
            if counter > 5:
                should_try = False
                p.kill()
                raise
            LOGGER.warning(
                "db-sync progress unavailable, possible postgres failure. Output from psql: %s",
                progress_string,
            )
            counter += 1
            time.sleep(ONE_MINUTE)
        else:
            return db_sync_progress
    return None


def get_total_db_size(config: DbSyncConfig) -> str:
    """Fetch the total size of the Cardano DB Sync database.

    Args:
        config: A DbSyncConfig instance with database connection settings.

    Returns:
        str: Human-readable database size.
    """
    cmd = [
        "psql",
        "-P",
        "pager=off",
        "-qt",
        "-U",
        config.pg_user,
        "-d",
        config.pg_dbname,
        "-c",
        "SELECT pg_size_pretty(pg_database_size(current_database()));",
    ]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if result.stderr:
        LOGGER.error("Error in get database size: %s", result.stderr.strip())
    if result.returncode != 0:
        msg = f"Failed to get database size via psql (exit {result.returncode})"
        raise RuntimeError(msg)
    return result.stdout.strip()


def get_db_schema(config: DbSyncConfig) -> dict:
    """Retrieve the schema of the Cardano DB Sync database.

    Args:
        config: A DbSyncConfig instance with database connection settings.

    Returns:
        dict: A dictionary mapping table names to their schema definitions.
    """
    conn = None
    try:
        conn = psycopg2.connect(database=config.pg_dbname, user=config.pg_user)
        cursor = conn.cursor()
        get_all_tables = (
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        )
        cursor.execute(get_all_tables)
        tabels = cursor.fetchall()

        db_schema = {}
        for table in tabels:
            table_name = table[0]
            get_table_fields_and_attributes = (
                'SELECT a.attname as "Column", pg_catalog.format_type(a.atttypid, a.atttypmod)'
                ' as "Datatype" FROM pg_catalog.pg_attribute a WHERE a.attnum > 0'
                " AND NOT a.attisdropped AND a.attrelid = ( SELECT c.oid"
                " FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n"
                f" ON n.oid = c.relnamespace WHERE c.relname ~ '^{table_name}$'"
                " AND pg_catalog.pg_table_is_visible(c.oid));"
            )
            cursor.execute(get_table_fields_and_attributes)
            table_with_attributes = cursor.fetchall()
            attributes = list(table_with_attributes)
            db_schema[str(table_name)] = attributes
        cursor.close()
        conn.commit()
        conn.close()
    except (Exception, psycopg2.DatabaseError):
        LOGGER.exception("Error")
    finally:
        if conn is not None:
            conn.close()

    return db_schema


def get_db_indexes(config: DbSyncConfig) -> dict:
    """Fetch the indexes of tables in the Cardano DB Sync database.

    Args:
        config: A DbSyncConfig instance with database connection settings.

    Returns:
        dict: A dictionary mapping table names to their index names.
    """
    conn = None
    try:
        conn = psycopg2.connect(database=config.pg_dbname, user=config.pg_user)
        cursor = conn.cursor()

        get_all_tables = (
            "select tbl.relname as table_name from pg_index pgi"
            " join pg_class idx on idx.oid = pgi.indexrelid join pg_namespace insp"
            " on insp.oid = idx.relnamespace join pg_class tbl"
            " on tbl.oid = pgi.indrelid join pg_namespace tnsp"
            " on tnsp.oid = tbl.relnamespace where pgi.indisunique and tnsp.nspname = 'public';"
        )
        cursor.execute(get_all_tables)
        tables = cursor.fetchall()
        all_indexes = {}

        for table in tables:
            table_name = table[0]
            get_table_and_index = (
                "select tbl.relname as table_name, idx.relname as index_name"
                " from pg_index pgi join pg_class idx on idx.oid = pgi.indexrelid"
                " join pg_namespace insp on insp.oid = idx.relnamespace join pg_class tbl"
                " on tbl.oid = pgi.indrelid join pg_namespace tnsp"
                " on tnsp.oid = tbl.relnamespace where pgi.indisunique"
                f" and tnsp.nspname = 'public' and tbl.relname = '{table_name}';"
            )
            cursor.execute(get_table_and_index)
            table_and_index = cursor.fetchall()
            indexes = []
            for _table, index in table_and_index:
                indexes.append(index)
            all_indexes[str(table_name)] = indexes
        cursor.close()
        conn.commit()
        conn.close()
    except (Exception, psycopg2.DatabaseError):
        LOGGER.exception("Error")
        return {}
    else:
        return all_indexes
    finally:
        if conn is not None:
            conn.close()


def check_database(fn: tp.Callable, err_msg: str, expected_value: tp.Any) -> Exception | None:
    """Validate the database using a specified function and expected value."""
    actual = fn()
    if actual != expected_value:
        msg = f"{err_msg}: expected {expected_value!r}, got {actual!r}"
        e = ValueError(msg)
        helpers.print_message(f"Warning - validation errors: {e}\n\n", type="warn")
        return e
    return None


def get_era_activation_data(config: DbSyncConfig) -> list[dict]:
    """Return era activation metadata derived from db-sync tables."""
    query = """
    WITH EraFirstEpoch AS (
        -- Dynamically find the activation epoch for every protocol version
        SELECT
            actual_proto_major,
            MIN(epoch_no) as epoch_no
        FROM (
            -- For modern eras (2+), epoch_param is the official record of activation
            SELECT epoch_no, protocol_major as actual_proto_major FROM epoch_param
            UNION
            -- For Byron (0 and 1), find first epoch where each version is dominant
            SELECT MIN(epoch_no), proto_major
            FROM block
            WHERE proto_major = 0
            GROUP BY proto_major
            UNION
            SELECT MIN(epoch_no), proto_major
            FROM block
            WHERE proto_major = 1
              AND epoch_no > (SELECT MAX(epoch_no) FROM block WHERE proto_major = 0)
            GROUP BY proto_major
        ) s
        GROUP BY actual_proto_major
    )
    SELECT DISTINCT ON (efe.actual_proto_major)
        efe.actual_proto_major AS protocol_version,
        CASE
            WHEN efe.actual_proto_major = 0 THEN 'Byron (Genesis)'
            WHEN efe.actual_proto_major = 1 THEN 'Byron (Reboot)'
            WHEN efe.actual_proto_major = 2 THEN 'Shelley'
            WHEN efe.actual_proto_major = 3 THEN 'Allegra'
            WHEN efe.actual_proto_major = 4 THEN 'Mary'
            WHEN efe.actual_proto_major = 5 THEN 'Alonzo'
            WHEN efe.actual_proto_major = 6 THEN 'Alonzo (Intra-Era)'
            WHEN efe.actual_proto_major = 7 THEN 'Babbage (Vasil)'
            WHEN efe.actual_proto_major = 8 THEN 'Babbage (Valentine)'
            WHEN efe.actual_proto_major = 9 THEN 'Conway (Chang 1)'
            WHEN efe.actual_proto_major = 10 THEN 'Chang 2 (Plomin)'
            ELSE 'Future Era'
        END AS era_name,
        efe.epoch_no AS activation_epoch,
        b.block_no AS first_block_number,
        b.slot_no AS absolute_slot,
        b.time AS activation_time_utc,
        encode(b.hash, 'hex') AS first_block_hash
    FROM EraFirstEpoch efe
    JOIN block b ON b.epoch_no = efe.epoch_no
    WHERE b.block_no IS NOT NULL
    ORDER BY efe.actual_proto_major ASC, b.block_no ASC;
    """

    conn = None
    try:
        conn = psycopg2.connect(database=config.pg_dbname, user=config.pg_user)
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        era_activation = []
        for row in rows:
            activation_time = row[5].isoformat() if row[5] else None
            era_activation.append(
                {
                    "protocol_version": row[0],
                    "era_name": row[1],
                    "activation_epoch": row[2],
                    "first_block_number": row[3],
                    "absolute_slot": row[4],
                    "activation_time_utc": activation_time,
                    "first_block_hash": row[6],
                }
            )
        cursor.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError):
        LOGGER.exception("Failed to query era activation data")
    else:
        return era_activation
    finally:
        if conn is not None:
            conn.close()
    return []
