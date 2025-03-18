import json
import logging
import os
import pathlib as pl
import sys
import typing as tp

import pymysql.cursors

from sync_tests.utils import db_sync

TEST_RESULTS = f"db_sync_{db_sync.ENVIRONMENT}_full_sync_test_results.json"

LOGGER = logging.getLogger(__name__)


def create_connection() -> pymysql.Connect | None:
    """Create and returns a database connection."""
    try:
        conn = pymysql.connect(
            host=os.environ.get("AWS_DB_HOSTNAME"),
            user=os.environ.get("AWS_DB_USERNAME"),
            password=os.environ.get("AWS_DB_PASS") or "",
            db=os.environ.get("AWS_DB_NAME"),
        )
    except Exception:
        LOGGER.exception("Database connection failed")
        return None
    else:
        return conn


def execute_query(
    conn: pymysql.Connect,
    query: str,
    params: tp.Iterable | None = None,
    fetch_one: bool = False,
    fetch_all: bool = False,
) -> tp.Any:
    """Execute a query and optionally fetches results."""
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch_one:
                return cur.fetchone()
            if fetch_all:
                return cur.fetchall()
            conn.commit()
    except Exception:
        LOGGER.exception(f"Query execution failed: {query}")
        return None


def get_column_names_from_table(table_name: str) -> list[str]:
    """Retrieve column names from the specified table."""
    LOGGER.info(f"Getting column names from table: {table_name}")
    conn = create_connection()
    if not conn:
        return []

    try:
        query = f"SELECT * FROM `{table_name}` LIMIT 1"
        execute_query(conn, query, fetch_one=True)
        return [desc[0] for desc in conn.cursor().description]
    finally:
        conn.close()


def add_column_to_table(table_name: str, column_name: str, column_type: str) -> bool:
    """Add a new column to the specified table."""
    LOGGER.info(f"Adding column {column_name} of type {column_type} to table {table_name}")
    conn = create_connection()
    if not conn:
        return False

    try:
        query = f"ALTER TABLE `{table_name}` ADD COLUMN `{column_name}` {column_type}"
        execute_query(conn, query)
        LOGGER.info(f"Successfully added column {column_name} to table {table_name}")
        return True
    finally:
        conn.close()


def insert_values_into_db(
    table_name: str, col_names_list: list[str], col_values_list: list, bulk: bool = False
) -> bool:
    """Insert values into the database table."""
    action = "bulk" if bulk else "single"
    LOGGER.info(
        f"Adding {len(col_values_list) if bulk else 1} {action} entry/entries into {table_name}"
    )

    col_names = ", ".join([f"`{col}`" for col in col_names_list])
    placeholders = ", ".join(["%s"] * len(col_names_list))

    query = f"INSERT INTO `{table_name}` ({col_names}) VALUES ({placeholders})"
    conn = create_connection()
    if not conn:
        return False

    try:
        cur = conn.cursor()
        if bulk:
            cur.executemany(query, col_values_list)
        else:
            cur.execute(query, col_values_list)
        conn.commit()
        LOGGER.info(f"Successfully added {cur.rowcount} rows to table {table_name}")
    except Exception:
        LOGGER.exception(f"Failed to insert data into {table_name}")
        return False
    finally:
        conn.close()
    return True


def get_row_count(table_name: str) -> int | None:
    """Return the row count of a table."""
    LOGGER.info(f"Getting row count for table: {table_name}")
    conn = create_connection()
    if not conn:
        return None

    try:
        query = f"SELECT COUNT(*) FROM `{table_name}`"
        result = execute_query(conn, query, fetch_one=True)
        return result[0] if result else 0
    finally:
        conn.close()


def get_last_identifier(table_name: str) -> str | None:
    """Get the last identifier value from the table."""
    LOGGER.info(f"Fetching last identifier value from {table_name}")

    if get_row_count(table_name) == 0:
        return f"{table_name}_0"

    conn = create_connection()
    if not conn:
        return None

    try:
        query = (
            f"SELECT identifier FROM `{table_name}` "
            f"ORDER BY LPAD(LOWER(identifier), 500, '0') DESC LIMIT 1"
        )
        result = execute_query(conn, query, fetch_one=True)
        return result[0] if result else None
    finally:
        conn.close()


def get_max_epoch(table_name: str) -> int | None:
    """Get the maximum epoch number from the table."""
    LOGGER.info(f"Fetching max epoch number from {table_name}")

    if get_row_count(table_name) == 0:
        return 0

    conn = create_connection()
    if not conn:
        return None

    try:
        query = f"SELECT MAX(epoch_no) FROM `{table_name}`"
        result = execute_query(conn, query, fetch_one=True)
        return result[0] if result else 0
    finally:
        conn.close()


def upload_sync_results_to_aws(env: str) -> None:
    os.chdir(db_sync.ROOT_TEST_PATH)
    os.chdir(pl.Path.cwd() / "cardano-db-sync")

    LOGGER.info("Writing full sync results to AWS Database")
    with open(TEST_RESULTS) as json_file:
        sync_test_results_dict = json.load(json_file)

    test_summary_table = env + "_db_sync"
    last_identifier = get_last_identifier(test_summary_table)
    assert last_identifier is not None  # TODO: refactor
    test_id = str(int(last_identifier.split("_")[-1]) + 1)
    identifier = env + "_" + test_id
    sync_test_results_dict["identifier"] = identifier

    LOGGER.info(f"Writing test values into {test_summary_table} DB table")
    col_to_insert = list(sync_test_results_dict.keys())
    val_to_insert = list(sync_test_results_dict.values())

    if not insert_values_into_db(
        table_name=test_summary_table,
        col_names_list=col_to_insert,
        col_values_list=val_to_insert,
    ):
        LOGGER.error(f"Failed to insert values into {test_summary_table}")
        sys.exit(1)

    with open(db_sync.EPOCH_SYNC_TIMES_FILE) as json_db_dump_file:
        epoch_sync_times = json.load(json_db_dump_file)

    epoch_duration_table = env + "_epoch_duration_db_sync"
    LOGGER.info(f"  ==== Write test values into the {epoch_duration_table} DB table:")
    col_to_insert = ["identifier", "epoch_no", "sync_duration_secs"]
    val_to_insert = [(identifier, e["no"], e["seconds"]) for e in epoch_sync_times]

    if not insert_values_into_db(
        table_name=epoch_duration_table,
        col_names_list=col_to_insert,
        col_values_list=val_to_insert,
        bulk=True,
    ):
        LOGGER.error(f"Failed to insert values into {epoch_duration_table}")
        sys.exit(1)

    with open(db_sync.DB_SYNC_PERF_STATS_FILE) as json_perf_stats_file:
        db_sync_performance_stats = json.load(json_perf_stats_file)

    db_sync_performance_stats_table = env + "_performance_stats_db_sync"
    LOGGER.info(f"  ==== Write test values into the {db_sync_performance_stats_table} DB table:")
    col_to_insert = [
        "identifier",
        "time",
        "slot_no",
        "cpu_percent_usage",
        "rss_mem_usage",
    ]
    val_to_insert = [
        (
            identifier,
            e["time"],
            e["slot_no"],
            e["cpu_percent_usage"],
            e["rss_mem_usage"],
        )
        for e in db_sync_performance_stats
    ]

    if not insert_values_into_db(
        table_name=db_sync_performance_stats_table,
        col_names_list=col_to_insert,
        col_values_list=val_to_insert,
        bulk=True,
    ):
        LOGGER.error(f"Failed to insert values into {db_sync_performance_stats_table}")
        sys.exit(1)
