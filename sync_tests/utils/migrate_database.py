import json

import pymysql
import pandas as pd

BATCH_SIZE = 500

def connect_to_database() -> pymysql.connections.Connection:
    """Connect to the database using AWS environment variables."""
    return pymysql.connect(
        host="database-1.csnxovlgjohu.us-east-2.rds.amazonaws.com",
        user="admin",
        password="Iog_Qa_db",
        db="qa_sync_tests_db",
    )

def get_old_data(table: str, cursor: pymysql.cursors.Cursor, batch_size=1000) -> pd.DataFrame:
    query = f"SELECT * FROM {table}"
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]

    data = []
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        data.extend(rows)

    return pd.DataFrame(data, columns=columns)


def create_table(
    query: str, cursor: pymysql.cursors.Cursor, conn: pymysql.connections.Connection
) -> None:
    cursor.execute(query)
    conn.commit()


def insert_data(
    table: str,
    data: pd.DataFrame,
    required_columns: list,
    cursor: pymysql.cursors.Cursor,
    conn: pymysql.connections.Connection
) -> None:
    # Insert data into the new table
    placeholders = ", ".join(["%s"] * len(required_columns))
    query = f"INSERT INTO {table} ({', '.join(required_columns)}) VALUES ({placeholders})"

    values = [
        tuple(None if pd.isna(row[col]) else row[col] for col in required_columns)
        for _, row in data.iterrows()
    ]

    try:
        for i in range(0, len(values), BATCH_SIZE):
            cursor.executemany(query, values[i: i + BATCH_SIZE])
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data: {e}")

def create_sync_run_table(env: str, cursor: pymysql.cursors.Cursor, conn: pymysql.connections.Connection):
    old_table = env

    # Keep only the relevant columns
    old_df = get_old_data(table=old_table, cursor=cursor)

    # Rename columns based on mapping
    column_mapping = {
        "identifier": "id",
        "cli_version1": "cli_revision1",
        "cli_version2": "cli_revision2",
        "hydra_eval_no1": "node_revision1",
        "hydra_eval_no2": "node_revision2",
    }
    new_df = old_df.rename(columns=column_mapping)

    # Ensure 'env' is dynamically assigned if missing
    if "env" not in new_df.columns or new_df["env"].isnull().all():
        new_df["env"] = env

    required_columns = [
        "id", "env", "tag_no1", "tag_no2", "cli_revision1", "cli_revision2",
        "node_revision1",
        "node_revision2",
        "start_sync_time1", "end_sync_time1", "start_sync_time2", "end_sync_time2",
        "last_slot_no1",
        "last_slot_no2",
        "start_node_secs1", "start_node_secs2", "sync_time_seconds1", "sync_time1",
        "sync_time_seconds2", "sync_time2",
        "total_chunks1", "total_chunks2", "platform_system", "platform_release",
        "platform_version",
        "chain_size_bytes",
        "eras_in_test", "no_of_cpu_cores", "epoch_no_d_zero", "total_ram_in_GB",
        "start_slot_no_d_zero"
    ]

    # Create the new table if it doesn't exist
    query = '''
    CREATE TABLE IF NOT EXISTS sync_run (
        id VARCHAR(255) PRIMARY KEY NOT NULL,
        env VARCHAR(255) NOT NULL,
        tag_no1 VARCHAR(255),
        tag_no2 VARCHAR(255),
        cli_revision1 VARCHAR(255) NOT NULL,
        cli_revision2 VARCHAR(255),
        node_revision1 VARCHAR(255) NOT NULL,
        node_revision2 VARCHAR(255),
        start_sync_time1 VARCHAR(255) NOT NULL,
        end_sync_time1 VARCHAR(255) NOT NULL,
        start_sync_time2 VARCHAR(255),
        end_sync_time2 VARCHAR(255),
        last_slot_no1 INTEGER,
        last_slot_no2 INTEGER,
        start_node_secs1 INTEGER,
        start_node_secs2 INTEGER,
        sync_time_seconds1 INTEGER,
        sync_time1 VARCHAR(255),
        sync_time_seconds2 INTEGER,
        sync_time2 VARCHAR(255),
        total_chunks1 INTEGER,
        total_chunks2 INTEGER,
        platform_system VARCHAR(255),
        platform_release VARCHAR(255),
        platform_version VARCHAR(255),
        chain_size_bytes BIGINT,
        eras_in_test VARCHAR(255),
        no_of_cpu_cores INTEGER,
        epoch_no_d_zero INTEGER,
        total_ram_in_GB INTEGER,
        start_slot_no_d_zero INTEGER
    );
    '''

    create_table(query=query, cursor=cursor, conn=conn)
    insert_data(
        table="sync_run", data=new_df, required_columns=required_columns, cursor=cursor, conn=conn
    )

def create_details_per_era_table(env: str, cursor: pymysql.cursors.Cursor, conn: pymysql.connections.Connection):
    old_table = env

    query = '''
    CREATE TABLE IF NOT EXISTS details_per_era (
        id INTEGER AUTO_INCREMENT PRIMARY KEY,
        sync_run_id VARCHAR(255) NOT NULL,
        era VARCHAR(255) NOT NULL,
        start_time DATETIME,
        start_epoch INTEGER,
        slots_in_era INTEGER,
        start_sync_time DATETIME,
        end_sync_time DATETIME,
        sync_duration_secs INTEGER,
        sync_speed_sps DECIMAL(10,2),
        FOREIGN KEY (sync_run_id) REFERENCES sync_run(id) ON DELETE CASCADE
    );
    '''

    create_table(query=query, cursor=cursor, conn=conn)

    old_df = get_old_data(table=old_table, cursor=cursor)

    eras = json.loads(old_df.eras_in_test[0])

    transformed_data = []

    for era in eras:
        # Rename columns based on mapping
        column_mapping = {
            "identifier": "sync_run_id",
            f"{era}_start_time": "start_time",
            f"{era}_start_epoch": "start_epoch",
            f"{era}_slots_in_era": "slots_in_era",
            f"{era}_start_sync_time": "start_sync_time",
            f"{era}_end_sync_time": "end_sync_time",
            f"{era}_sync_duration_secs": "sync_duration_secs",
            f"{era}_sync_speed_sps": "sync_speed_sps"
        }
        new_df = old_df.rename(columns=column_mapping)

        # Ensure 'era' is dynamically assigned if missing
        if "era" not in new_df.columns or new_df["era"].isnull().all():
            new_df["era"] = era

        # Convert data types to match MySQL schema
        for col in ["start_epoch", "slots_in_era", "sync_duration_secs"]:
            new_df[col] = pd.to_numeric(
                new_df[col], errors="coerce"
            ).astype("Int64")

        for col in ["start_time", "start_sync_time", "end_sync_time"]:
            new_df[col] = pd.to_datetime(
                new_df[col], errors="coerce", format="%Y-%m-%dT%H:%M:%SZ")

        new_df["sync_speed_sps"] = pd.to_numeric(
            new_df["sync_speed_sps"], errors="coerce"
        ).astype(float)

        # Append transformed rows
        transformed_data.append(new_df)

        required_columns = [
            "sync_run_id", "era", "start_time", "start_epoch", "slots_in_era",
            f"start_sync_time", f"end_sync_time",
            f"sync_duration_secs", f"sync_speed_sps"
        ]

        insert_data(
            table="details_per_era", data=new_df, required_columns=required_columns, cursor=cursor, conn=conn
        )

def create_epoch_duration_table(env: str, cursor: pymysql.cursors.Cursor, conn: pymysql.connections.Connection):
    old_table = f"{env}_epoch_duration"

    required_columns = ["sync_run_id", "epoch_no", "sync_duration_secs"]

    # Keep only the relevant columns
    old_df = get_old_data(table=old_table, cursor=cursor)

    # Rename columns based on mapping
    column_mapping = {"identifier": "sync_run_id"}
    new_df = old_df.rename(columns=column_mapping)

    query = '''
    CREATE TABLE IF NOT EXISTS epoch_duration (
        id INT AUTO_INCREMENT PRIMARY KEY,
        sync_run_id VARCHAR(255) NOT NULL,
        epoch_no INTEGER,
        sync_duration_secs INTEGER,
        FOREIGN KEY (sync_run_id) REFERENCES sync_run(id) ON DELETE CASCADE
    );
    '''

    create_table(query=query, cursor=cursor, conn=conn)

    insert_data(
        table="epoch_duration", data=new_df, required_columns=required_columns, cursor=cursor, conn=conn
    )

def create_system_metrics_table(env: str, cursor: pymysql.cursors.Cursor, conn: pymysql.connections.Connection):
    old_table = f"{env}_logs"

    required_columns = [
        "sync_run_id", "timestamp", "slot_no", "ram_bytes", "cpu_percent", "rss_ram_bytes"
    ]

    # Keep only the relevant columns
    old_df = get_old_data(table=old_table, cursor=cursor)

    # Rename columns based on mapping
    column_mapping = {"identifier": "sync_run_id"}
    new_df = old_df.rename(columns=column_mapping)

    # Convert empty strings to NaN to avoid conversion errors
    new_df.replace("", pd.NA, inplace=True)  # Handle empty strings

    # Convert timestamp column to DATETIME format
    new_df["timestamp"] = pd.to_datetime(new_df["timestamp"], errors='coerce', format="%Y-%m-%d %H:%M:%S")

    # Convert RAM values (scientific notation) to BIGINT (integers)
    new_df["ram_bytes"] = pd.to_numeric(new_df["ram_bytes"], errors='coerce').astype("Int64")

    if new_df.get("rss_ram_bytes"):
        new_df["rss_ram_bytes"] = pd.to_numeric(new_df["rss_ram_bytes"], errors='coerce').astype("Int64")

    # Convert CPU usage percentage to DECIMAL(5,2)
    new_df["cpu_percent"] = pd.to_numeric(new_df["cpu_percent"], errors='coerce').astype(float)

    query = '''
    CREATE TABLE IF NOT EXISTS system_metrics (
        id INT AUTO_INCREMENT PRIMARY KEY,
        sync_run_id VARCHAR(255) NOT NULL,
        timestamp DATETIME,
        slot_no INT NOT NULL,
        ram_bytes BIGINT,
        cpu_percent DECIMAL(5,2),
        rss_ram_bytes BIGINT,
        FOREIGN KEY (sync_run_id) REFERENCES sync_run(id) ON DELETE CASCADE
    );
    '''

    create_table(query=query, cursor=cursor, conn=conn)

    insert_data(
        table="system_metrics", data=new_df, required_columns=required_columns, cursor=cursor, conn=conn
    )

def create_new_tables_and_migrate_data():
    conn = connect_to_database()
    cursor = conn.cursor()

    envs = ["mainnet", "preprod", "preview", "staging", "testnet"]

    for env in envs:
        create_sync_run_table(env=env, cursor=cursor, conn=conn)
        create_details_per_era_table(env=env, cursor=cursor, conn=conn)
        create_epoch_duration_table(env=env, cursor=cursor, conn=conn)
        create_system_metrics_table(env=env, cursor=cursor, conn=conn)





def create_dbsync_sync_run_table(env: str, cursor: pymysql.cursors.Cursor, conn: pymysql.connections.Connection):
    old_table = f"{env}_db_sync"

    # Keep only the relevant columns
    old_df = get_old_data(table=old_table, cursor=cursor)

    # Rename columns based on mapping
    column_mapping = {
        "identifier": "id",
        "node_cli_version": "cli_revision",
        "node_git_revision": "node_revision",
        "db_sync_git_rev": "db_sync_revision",
    }
    new_df = old_df.rename(columns=column_mapping)

    # Ensure 'env' is dynamically assigned if missing
    if "env" not in new_df.columns or new_df["env"].isnull().all():
        new_df["env"] = env

    required_columns = [
        "id", "env", "cli_revision", "node_revision", "db_sync_revision", "start_test_time",
        "end_test_time",
        "total_sync_time_in_sec",
        "total_sync_time_in_h_m_s", "last_synced_epoch_no", "last_synced_block_no", "platform_system",
        "platform_release",
        "platform_version",
        "no_of_cpu_cores", "total_ram_in_GB", "total_database_size", "last_synced_slot_no",
        "rollbacks", "errors",
        "cpu_percent_usage", "total_rss_memory_usage_in_B"
    ]

    # Create the new table if it doesn't exist
    query = '''
    CREATE TABLE IF NOT EXISTS sync_run (
        id VARCHAR(255) PRIMARY KEY NOT NULL,
        env VARCHAR(255) NOT NULL,
        cli_revision VARCHAR(255) NOT NULL,
        node_revision VARCHAR(255) NOT NULL,
        db_sync_revision VARCHAR(255) NOT NULL,
        start_test_time VARCHAR(255),
        end_test_time VARCHAR(255),
        total_sync_time_in_h_m_s INT,
        start_sync_time1 VARCHAR(255) NOT NULL,
        end_sync_time1 VARCHAR(255) NOT NULL,
        last_synced_epoch_no INT,
        last_synced_block_no INT,
        platform_system VARCHAR(255),
        platform_release VARCHAR(255),
        platform_version VARCHAR(255),
        no_of_cpu_cores INT,
        total_ram_in_GB INT,
        total_database_size VARCHAR(255),
        last_synced_slot_no INT,
        rollbacks VARCHAR(255),
        errors VARCHAR(255),
        cpu_percent_usage DECIMAL(5,2),
        total_rss_memory_usage_in_B BIGINT
    );
    '''

    create_table(query=query, cursor=cursor, conn=conn)
    insert_data(
        table="dbsync_sync_run", data=new_df, required_columns=required_columns, cursor=cursor, conn=conn
    )

def create_dbsync_epoch_duration_table(env: str, cursor: pymysql.cursors.Cursor, conn: pymysql.connections.Connection):
    old_table = f"{env}_epoch_duration_dbsync"

    required_columns = ["dbsync_sync_run_id", "epoch_no", "sync_duration_secs"]

    # Keep only the relevant columns
    old_df = get_old_data(table=old_table, cursor=cursor)

    # Rename columns based on mapping
    column_mapping = {"identifier": "dbsync_sync_run_id"}
    new_df = old_df.rename(columns=column_mapping)

    query = '''
    CREATE TABLE IF NOT EXISTS epoch_duration (
        id INT AUTO_INCREMENT PRIMARY KEY,
        dbsync_sync_run VARCHAR(255) NOT NULL,
        epoch_no INTEGER,
        sync_duration_secs INTEGER,
        FOREIGN KEY (dbsync_sync_run_id) REFERENCES dbsync_sync_run(id) ON DELETE CASCADE
    );
    '''

    create_table(query=query, cursor=cursor, conn=conn)

    insert_data(
        table="dbsync_epoch_duration", data=new_df, required_columns=required_columns, cursor=cursor, conn=conn
    )

def create_db_sync_system_metrics_table(env: str, cursor: pymysql.cursors.Cursor, conn: pymysql.connections.Connection):
    old_table = f"{env}_performance_stats_db_sync"

    required_columns = [
        "dbsync_sync_run_id", "time", "slot_no", "cpu_percent", "rss_ram_bytes"
    ]

    # Keep only the relevant columns
    old_df = get_old_data(table=old_table, cursor=cursor)

    # Rename columns based on mapping
    column_mapping = {
        "identifier": "dbsync_sync_run_id",
        "time": "timestamp",
        "cpu_percent_usage": "cpu_percent",
        "rss_mem_usage": "rss_ram_bytes"
    }
    new_df = old_df.rename(columns=column_mapping)

    # Convert empty strings to NaN to avoid conversion errors
    new_df.replace("", pd.NA, inplace=True)  # Handle empty strings

    # Convert timestamp column to DATETIME format
    new_df["timestamp"] = pd.to_datetime(new_df["timestamp"], errors='coerce', format="%Y-%m-%d %H:%M:%S")

    if new_df.get("rss_mem_usage"):
        new_df["rss_mem_usage"] = pd.to_numeric(new_df["rss_mem_usage"], errors='coerce').astype("Int64")

    # Convert CPU usage percentage to DECIMAL(5,2)
    new_df["cpu_percent"] = pd.to_numeric(new_df["cpu_percent"], errors='coerce').astype(float)

    query = '''
    CREATE TABLE IF NOT EXISTS system_metrics (
        id INT AUTO_INCREMENT PRIMARY KEY,
        dbsync_sync_run VARCHAR(255) NOT NULL,
        timestamp DATETIME,
        cpu_percent DECIMAL(5,2),
        rss_ram_bytes BIGINT,
        FOREIGN KEY (dbsync_sync_run_id) REFERENCES dbsync_sync_run(id) ON DELETE CASCADE
    );
    '''

    create_table(query=query, cursor=cursor, conn=conn)

    insert_data(
        table="system_metrics", data=new_df, required_columns=required_columns, cursor=cursor, conn=conn
    )

if __name__ == "__main__":
    create_new_tables_and_migrate_data()
