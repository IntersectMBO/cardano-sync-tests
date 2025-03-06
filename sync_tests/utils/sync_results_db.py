import logging
import os

import pymysql.cursors

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


def insert_sync_run_entry(test_values: dict, cursor: pymysql.cursors.Cursor) -> str:
    """Insert a new entry into the sync_run table using the provided test_values dictionary."""
    sync_run_id = ""
    try:
        # Fetch column names dynamically
        cursor.execute("SHOW COLUMNS FROM sync_run")
        valid_columns = {row[0] for row in cursor.fetchall()}

        # Filter dictionary to include only valid columns
        filtered_data = {key: value for key, value in test_values.items() if key in valid_columns}

        # Generate sync_run_id safely
        sync_run_id = (
            f"{test_values['env']}_{test_values['node_revision1']}_"
            f"{test_values['start_sync_time1']}"
        )
        filtered_data["id"] = sync_run_id  # Ensure ID is added to the data

        # Construct SQL query dynamically
        columns = ", ".join(filtered_data.keys())  # Get column names
        placeholders = ", ".join(["%s"] * len(filtered_data))  # Generate placeholders
        sql = f"INSERT INTO sync_run ({columns}) VALUES ({placeholders})"

        # Execute the query
        cursor.execute(sql, tuple(filtered_data.values()))

        LOGGER.info(f"Inserted sync_run entry with ID: {sync_run_id}")
    except Exception:
        LOGGER.exception("Failed to insert sync_run entry into the database")

    return sync_run_id


def insert_details_per_era_entries(
    sync_run_id: str, test_values: dict, cursor: pymysql.cursors.Cursor
) -> None:
    """Insert multiple 'details_per_era' records into the database dynamically."""
    try:
        # Fetch valid columns once
        cursor.execute("SHOW COLUMNS FROM details_per_era")
        valid_columns = {row[0] for row in cursor.fetchall()}

        # Ensure mandatory columns exist
        valid_columns.update(["sync_run_id", "era"])

        # Prepare filtered data for batch insert
        data_to_insert = []
        for era, era_data in test_values.items():
            filtered_data = {key: value for key, value in era_data.items() if key in valid_columns}
            filtered_data["sync_run_id"] = sync_run_id
            filtered_data["era"] = era

            data_to_insert.append(tuple(filtered_data.values()))

        if not data_to_insert:
            return  # Nothing to insert after filtering

        # Generate SQL dynamically
        columns = ", ".join(filtered_data.keys())  # Get column names
        placeholders = ", ".join(["%s"] * len(filtered_data))  # Generate placeholders
        sql = f"INSERT INTO details_per_era ({columns}) VALUES ({placeholders})"

        # Execute batch insert
        cursor.executemany(sql, data_to_insert)

        LOGGER.info(f"Inserted {len(data_to_insert)} rows into details_per_era table.")
    except Exception:
        LOGGER.exception("Failed to insert details_per_era entries into the database")


def insert_epoch_duration_entries(
    sync_run_id: str, test_values: dict, cursor: pymysql.cursors.Cursor
) -> None:
    """Insert multiple epoch duration records into the 'epoch_duration' table."""
    try:
        # Prepare SQL statement
        sql = """
        INSERT INTO epoch_duration (sync_run_id, epoch_no, sync_duration_secs)
        VALUES (%s, %s, %s)
        """

        # Prepare data for batch insertion
        data_to_insert = [(sync_run_id, epoch, duration) for epoch, duration in test_values.items()]

        # Execute batch insert
        cursor.executemany(sql, data_to_insert)

        LOGGER.info(f"Inserted {len(data_to_insert)} rows into epoch_duration table.")
    except Exception:
        LOGGER.exception("Failed to insert epoch_duration entries into the database")


def insert_system_metrics_entries(
    sync_run_id: str, test_values: dict, cursor: pymysql.cursors.Cursor
) -> None:
    """Insert multiple rows into the system_metrics table using log_values_dict."""
    try:
        # SQL query template
        sql = """
            INSERT INTO system_metrics (
                sync_run_id, timestamp, slot_no, ram_bytes,
                cpu_percent, rss_ram_bytes
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """

        # Prepare data for insertion
        data_to_insert = []
        for key, val in test_values.items():
            row = (
                sync_run_id,
                key,
                val.get("tip"),
                val.get("heap_ram"),
                val.get("cpu"),
                val.get("rss_ram"),
            )
            data_to_insert.append(row)

        # Execute batch insert
        cursor.executemany(sql, data_to_insert)

        LOGGER.info(f"Inserted {len(data_to_insert)} rows into system_metrics table.")
    except Exception:
        LOGGER.exception("Failed to insert system_metrics entries into the database")
