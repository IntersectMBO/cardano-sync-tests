from __future__ import annotations

import logging
import os
import pathlib as pl
import subprocess
import time
import typing as tp

import psycopg2
from assertpy import assert_that

from sync_tests.utils import artifacts
from sync_tests.utils import helpers
from sync_tests.utils.db_sync_config import DbSyncConfig
from sync_tests.utils.db_sync_config import DbSyncTip

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

    # Script is in repository root, not in workdir
    # Use __file__ to find repo root (this file is at sync_tests/utils/postgres.py)
    # Go up 2 levels from this file to get repo root
    repo_root = pl.Path(__file__).parent.parent.parent
    script_path = repo_root / "sync_tests" / "scripts" / "postgres-start.sh"
    try:
        cmd = [str(script_path), str(config.pg_dir), "-k"]
        output = (
            subprocess.check_output(cmd, cwd=str(config.workdir), stderr=subprocess.STDOUT)
            .decode("utf-8")
            .strip()
        )
        LOGGER.info(f"Setup postgres script output: {output}")
    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e


def create_pgpass_file(config: DbSyncConfig) -> None:
    """Create a PostgreSQL password file for the specified environment.

    Args:
        config: A DbSyncConfig instance with paths and PostgreSQL settings.
    """
    # cardano-db-sync is cloned to repository root, not config.workdir
    # Use __file__ to find repo root (this file is at sync_tests/utils/postgres.py)
    # Go up 2 levels from this file to get repo root
    repo_root = pl.Path(__file__).parent.parent.parent
    db_sync_config_dir = repo_root / "cardano-db-sync" / "config"
    db_sync_config_dir.mkdir(parents=True, exist_ok=True)

    pgpass_file = db_sync_config_dir / f"pgpass-{config.env}"
    postgres_port = os.getenv("PGPORT", config.pg_port)
    pgpass_content = f"{config.pg_dir}:{postgres_port}:{config.pg_dbname}:{config.pg_user}:*"
    helpers.export_env_var("PGPASSFILE", str(pgpass_file))

    with open(pgpass_file, "w") as pgpass_text_file:
        print(pgpass_content, file=pgpass_text_file)
    os.chmod(pgpass_file, 0o600)


def create_database(_config: DbSyncConfig) -> None:
    """Set up the PostgreSQL database for use with Cardano DB Sync.

    Args:
        config: A DbSyncConfig instance with paths and settings.
    """
    # cardano-db-sync is cloned to repository root, not config.workdir
    # Use __file__ to find repo root (this file is at sync_tests/utils/postgres.py)
    # Go up 2 levels from this file to get repo root
    repo_root = pl.Path(__file__).parent.parent.parent
    db_sync_dir = repo_root / "cardano-db-sync"
    script_path = db_sync_dir / "scripts" / "postgresql-setup.sh"

    try:
        cmd = [str(script_path), "--createdb"]
        output = (
            subprocess.check_output(cmd, cwd=str(db_sync_dir), stderr=subprocess.STDOUT)
            .decode("utf-8")
            .strip()
        )
        LOGGER.info(f"Create database script output: {output}")
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
        LOGGER.info(f"List databases: {outs}")
        if errs:
            LOGGER.error(f"Error in list databases: {errs}")
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
            "select epoch_no, block_no, slot_no from block order by id desc limit 1;",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    should_try = True
    counter = 0

    while should_try:
        output_string = ""
        try:
            outs, errs = p.communicate(timeout=180)
            output_string = outs.decode("utf-8").strip()
            err_msg = errs.decode("utf-8").strip() if errs else ""

            # Check for PostgreSQL connection errors
            if err_msg and ("connection" in err_msg.lower() or "fatal" in err_msg.lower()):
                msg = f"PostgreSQL connection error: {err_msg}"
                raise RuntimeError(msg)

            # Check if query returned empty (no blocks synced yet)
            if not output_string or output_string.isspace():
                if counter > 5:
                    LOGGER.warning(
                        "No blocks found in database yet - db-sync may not have started syncing"
                    )
                    return None
                # Not an error yet - db-sync might just be starting up
                LOGGER.debug("No blocks in database yet (attempt %s/6), waiting...", counter + 1)
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
                        "select epoch_no, block_no, slot_no from block order by id desc limit 1;",
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
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
        except Exception as e:
            if counter > 5:
                should_try = False
                artifacts.emergency_upload_artifacts(config, [])
                LOGGER.exception("Failed to get the tip after multiple retries")
                p.kill()
                raise
            err_msg = errs.decode("utf-8").strip() if errs else str(e)
            LOGGER.warning(
                f"db-sync tip data unavailable (attempt {counter + 1}/6). "
                f"Output from psql: '{output_string}', errs: '{err_msg}'. Retrying..."
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
                    "select epoch_no, block_no, slot_no from block order by id desc limit 1;",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

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
                artifacts.emergency_upload_artifacts(config, [])
                p.kill()
                raise
            LOGGER.exception(
                "db-sync progress unavailable, possible postgress failure. "
                f"Output from psql: {progress_string}"
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
        f"SELECT pg_size_pretty( pg_database_size('{config.pg_dbname}') );",
    ]
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        outs, errs = p.communicate(timeout=60)
        if errs:
            LOGGER.error(f"Error in get database size: {errs}")
        return outs.rstrip().strip()
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        p.kill()
        raise
    except Exception:
        p.kill()
        raise


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
    try:
        assert_that(fn()).described_as(err_msg).is_equal_to(expected_value)
    except AssertionError as e:
        helpers.print_message(f"Warning - validation errors: {e}\n\n", type="warn")
        return e
    return None
