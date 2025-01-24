import logging
import os

import pymysql.cursors

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def create_connection():
    """Creates and returns a database connection."""
    try:
        conn = pymysql.connect(
            host=os.environ.get("AWS_DB_HOSTNAME"),
            user=os.environ.get("AWS_DB_USERNAME"),
            password=os.environ.get("AWS_DB_PASS"),
            db=os.environ.get("AWS_DB_NAME"),
        )
        return conn
    except Exception as e:
        logging.exception(f"Database connection failed: {e}")
        return None


def execute_query(conn, query, params=None, fetch_one=False, fetch_all=False):
    """Executes a query and optionally fetches results."""
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch_one:
                return cur.fetchone()
            if fetch_all:
                return cur.fetchall()
            conn.commit()
    except Exception as e:
        logging.exception(f"Query execution failed: {query} -> {e}")
        return None


def get_column_names_from_table(table_name):
    """Retrieves column names from the specified table."""
    logging.info(f"Getting column names from table: {table_name}")
    conn = create_connection()
    if not conn:
        return False

    try:
        query = f"SELECT * FROM `{table_name}` LIMIT 1"
        execute_query(conn, query, fetch_one=True)
        return [desc[0] for desc in conn.cursor().description]
    finally:
        conn.close()


def add_column_to_table(table_name, column_name, column_type):
    """Adds a new column to the specified table."""
    logging.info(f"Adding column {column_name} of type {column_type} to table {table_name}")
    conn = create_connection()
    if not conn:
        return False

    try:
        query = f"ALTER TABLE `{table_name}` ADD COLUMN `{column_name}` {column_type}"
        execute_query(conn, query)
        logging.info(f"Successfully added column {column_name} to table {table_name}")
        return True
    finally:
        conn.close()


def insert_values_into_db(table_name, col_names_list, col_values_list, bulk=False):
    """Inserts values into the database table."""
    action = "bulk" if bulk else "single"
    logging.info(
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
        logging.info(f"Successfully added {cur.rowcount} rows to table {table_name}")
        return True
    except Exception as e:
        logging.exception(f"Failed to insert data into {table_name}: {e}")
        return False
    finally:
        conn.close()


def get_row_count(table_name):
    """Returns the row count of a table."""
    logging.info(f"Getting row count for table: {table_name}")
    conn = create_connection()
    if not conn:
        return False

    try:
        query = f"SELECT COUNT(*) FROM `{table_name}`"
        result = execute_query(conn, query, fetch_one=True)
        return result[0] if result else 0
    finally:
        conn.close()


def get_last_identifier(table_name):
    """Gets the last identifier value from the table."""
    logging.info(f"Fetching last identifier value from {table_name}")

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


def get_max_epoch(table_name):
    """Gets the maximum epoch number from the table."""
    logging.info(f"Fetching max epoch number from {table_name}")

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
