import os
import pymysql.cursors
import sync_tests.utils.utils as utils


def create_connection():
    conn = None
    try:
        conn = pymysql.connect(host=os.environ["AWS_DB_HOSTNAME"],
                               user=os.environ["AWS_DB_USERNAME"],
                               password=os.environ["AWS_DB_PASS"],
                               db=os.environ["AWS_DB_NAME"],
                               )
        return conn
    except Exception as e:
        utils.print_message(type="error", message=f"!!! Database connection failed due to: {e}")

    return conn


def get_column_names_from_table(table_name):
    utils.print_message(type="info", message=f"Getting the column names from table: {table_name}")

    conn = create_connection()
    sql_query = f"select * from {table_name}"
    print(f"  -- sql_query: {sql_query}")
    try:
        cur = conn.cursor()
        cur.execute(sql_query)
        col_name_list = [res[0] for res in cur.description]
        return col_name_list
    except Exception as e:
        utils.print_message(type="error", message=f"!!! ERROR: Failed to get column names from table: {table_name}: {e}")
        return False
    finally:
        if conn:
            conn.close()


def add_column_to_table(table_name, column_name, column_type):
    utils.print_message(type="info", message=f"Adding column {column_name} with type {column_type} to {table_name} table")

    conn = create_connection()
    sql_query = f"alter table {table_name} add column {column_name} {column_type}"
    print(f"  -- sql_query: {sql_query}")
    try:
        cur = conn.cursor()
        cur.execute(sql_query)
    except Exception as e:
        utils.print_message(type="error", message=f"!!! ERROR: Failed to add {column_name} column into table {table_name} --> {e}")
        return False
    finally:
        if conn:
            conn.close()


def add_single_value_into_db(table_name, col_names_list, col_values_list):
    utils.print_message(type="info", message=f"Adding 1 new entry into {table_name} table")
    initial_rows_no = get_last_row_no(table_name)
    col_names = ','.join(col_names_list)
    col_spaces = ','.join(['%s'] * len(col_names_list))
    conn = create_connection()
    sql_query = f"INSERT INTO {table_name} (%s) values(%s)" % (col_names, col_spaces)
    print(f"  -- sql_query: {sql_query}")
    try:
        cur = conn.cursor()
        cur.execute(sql_query, col_values_list)
        conn.commit()
        cur.close()
    except Exception as e:
        utils.print_message(type="error", message=f"  -- !!! ERROR: Failed to insert data into {table_name} table: {e}")
        return False
    finally:
        if conn:
            conn.close()
    final_rows_no = get_last_row_no(table_name)

    if final_rows_no > 0:
        utils.print_message(type="ok", message=f"Successfully added {final_rows_no - initial_rows_no} rows into table {table_name}")
        return True
    
    utils.print_message(type="error", message=f"Rows were NOT inserted ! final_rows_no: {final_rows_no}")
    return False


def add_bulk_values_into_db(table_name, col_names_list, col_values_list):
    utils.print_message(type="info", message=f"Adding {len(col_values_list)} entries into {table_name} table")
    initial_rows_no = get_last_row_no(table_name)
    col_names = ','.join(col_names_list)
    col_spaces = ','.join(['%s'] * len(col_names_list))
    conn = create_connection()
    sql_query = f"INSERT INTO {table_name} (%s) values (%s)" % (col_names, col_spaces)
    print(f"  -- sql_query: {sql_query}")
    try:
        cur = conn.cursor()
        cur.executemany(sql_query, col_values_list)
        conn.commit()
        cur.close()
    except Exception as e:
        utils.print_message(type="error", message=f"  -- !!! ERROR: Failed to bulk insert data into {table_name} table: {e}")
        return False
    finally:
        if conn:
            conn.close()
    final_rows_no = get_last_row_no(table_name)
    if final_rows_no > 0:
        utils.print_message(type="ok", message=f"Successfully added {final_rows_no - initial_rows_no} rows into table {table_name}")
        return True
    
    utils.print_message(type="error", message=f"Rows were NOT inserted ! final_rows_no: {final_rows_no}")
    return False


def get_last_row_no(table_name):
    utils.print_message(type="info", message=f"Getting the no of rows from table: {table_name}")

    conn = create_connection()
    sql_query = f"SELECT count(*) FROM {table_name};"
    print(f"  -- sql_query: {sql_query}")
    try:
        cur = conn.cursor()
        cur.execute(sql_query)
        last_row_no = cur.fetchone()[0]
        return last_row_no
    except Exception as e:
        utils.print_message(type="error", message=f"!!! ERROR: Failed to get the no of rows from table {table_name} --> {e}")
        return False
    finally:
        if conn:
            conn.close()


def get_identifier_last_run_from_table(table_name):
    utils.print_message(type="info", message=f"Getting the Identifier value of the last run from table {table_name}")

    if get_last_row_no(table_name) == 0:
        return table_name + "_0"
    else:
        conn = create_connection()
        sql_query = f"SELECT identifier FROM {table_name} " \
                    f"ORDER BY LPAD(LOWER(identifier), 500,0) DESC LIMIT 1;"
        print(f"  -- sql_query: {sql_query}")
        try:
            cur = conn.cursor()
            cur.execute(sql_query)
            last_identifier = cur.fetchone()[0]
            return last_identifier
        except Exception as e:
            utils.print_message(type="error", message=f"!!! ERROR: Failed to get the no of rows from table {table_name} --> {e}")
            return False
        finally:
            if conn:
                conn.close()


def get_last_epoch_no_from_table(table_name):
    utils.print_message(type="info", message=f"Getting the last epoch no value from table {table_name}")

    if get_last_row_no(table_name) == 0:
        return 0
    else:
        conn = create_connection()
        sql_query = f"SELECT MAX(epoch_no) FROM {table_name};;"
        print(f"  -- sql_query: {sql_query}")
        try:
            cur = conn.cursor()
            cur.execute(sql_query)
            last_identifier = cur.fetchone()[0]
            return last_identifier
        except Exception as e:
            utils.print_message(type="error", message=f"!!! ERROR: Failed to get last epoch no from table {table_name} --> {e}")
            return False
        finally:
            if conn:
                conn.close()


def add_single_row_into_db(table_name, col_names_list, col_values_list):
    print(f"Adding 1 new entry into {table_name} table")
    initial_rows_no = get_last_row_no(table_name)
    col_names = ','.join(col_names_list)
    col_spaces = ','.join(['%s'] * len(col_names_list))
    conn = create_connection()
    sql_query = f"INSERT INTO `{table_name}` (%s) values(%s)" % (col_names, col_spaces)
    print(f"  -- sql_query: {sql_query}")
    try:
        cur = conn.cursor()
        cur.execute(sql_query, col_values_list)
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"  -- !!! ERROR: Failed to insert data into {table_name} table: {e}")
        return False
    finally:
        if conn:
            conn.close()
    final_rows_no = get_last_row_no(table_name)
    print(f"Successfully added {final_rows_no - initial_rows_no} rows into table {table_name}")
    return True


def add_bulk_rows_into_db(table_name, col_names_list, col_values_list):
    print(f"Adding {len(col_values_list)} entries into {table_name} table")
    initial_rows_no = get_last_row_no(table_name)
    col_names = ','.join(col_names_list)
    col_spaces = ','.join(['%s'] * len(col_names_list))
    conn = create_connection()
    sql_query = f"INSERT INTO `{table_name}` (%s) values (%s)" % (col_names, col_spaces)
    print(f"  -- sql_query: {sql_query}")
    try:
        cur = conn.cursor()
        cur.executemany(sql_query, col_values_list)
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"  -- !!! ERROR: Failed to bulk insert data into {table_name} table: {e}")
        return False
    finally:
        if conn:
            conn.close()
    final_rows_no = get_last_row_no(table_name)
    print(f"Successfully added {final_rows_no - initial_rows_no} rows into table {table_name}")
    return True
