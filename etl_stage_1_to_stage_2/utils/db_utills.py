import psycopg2
import io
import time
import csv


# Database connection settings
DB_STAGE1 = {
    "dbname": "stage1",
    "user": "user",
    "password": "password",
    "host": "db_stage1",
    "port": "5432",
}

DB_STAGE2 = {
    "dbname": "stage2",
    "user": "user",
    "password": "password",
    "host": "db_stage2",
    "port": "5432",
}


# Database connection settings
DB_ADW = {
    "dbname": "adw",
    "user": "user",
    "password": "password",
    "host": "db_adw",
    "port": "5432",
}


def connect_to_db(db_params=DB_STAGE1, max_retries=5, retry_delay=5):
    """Attempt to connect to the PostgreSQL database with retries."""
    retries = 0
    while retries < max_retries:
        try:
            conn = psycopg2.connect(**db_params)
            # print("Database connection established.")
            return conn
        except psycopg2.OperationalError as e:
            print(
                f"Database connection failed: {e}. Retrying in {retry_delay} seconds..."
            )
            time.sleep(retry_delay)  # Wait before retrying
            retries += 1
    print("Max retries reached. Could not connect to the database.")
    raise Exception("Unable to connect to the database after several retries.")


def write_to_db(cursor, table_name, columns, data):
    """
    Writes a list of transformed rows to the database using COPY FROM.

    :param cursor: Database cursor.
    :param table_name: Name of the target table.
    :param columns: List of column names in the table.
    :param data: List of tuples representing transformed rows.
    """
    if not data:
        return  # Nothing to write

    output = io.StringIO()
    for row in data:
        output.write("\t".join(map(str, row)) + "\n")
    output.seek(0)

    try:
        cursor.copy_from(output, table_name, sep="\t", columns=columns)
    except Exception as e:
        raise Exception(f"Database write error: {e}")
