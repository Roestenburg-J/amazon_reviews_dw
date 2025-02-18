import psycopg2
import io
import time


def create_import_batch(conn, description, year, month, status):
    """Creates a new import batch row and retrieves the current import batch ID."""
    query = """
        INSERT INTO import_batch (ib_description, ib_year, ib_month, ib_start, ib_status)
        VALUES (%s, %s, %s, NOW(), %s)
        RETURNING ib_id;
    """

    with conn.cursor() as cur:
        cur.execute(query, (description, year, month, status))
        ib_id = cur.fetchone()[0]
        conn.commit()

    return ib_id


def update_import_batch(conn, ib_id, status):
    """Updates the status and end timestamp of an existing import batch."""
    query = """
        UPDATE import_batch 
        SET ib_status = %s, ib_end = NOW()
        WHERE ib_id = %s;
    """

    with conn.cursor() as cur:
        cur.execute(query, (status, ib_id))
        conn.commit()


def create_import_batch_process(conn, ib_id, description, status):
    """Creates a new import batch process and returns its ID."""
    query = """
        INSERT INTO import_batch_process (ib_id, ib_description, ib_status, ib_start)
        VALUES (%s, %s, %s, NOW())
        RETURNING ibp_id;
    """

    with conn.cursor() as cur:
        cur.execute(query, (ib_id, description, status))
        ibp_id = cur.fetchone()[0]
        conn.commit()

    return ibp_id


def update_import_batch_process(conn, ibp_id, status):
    """Updates the status and end timestamp of an import batch process."""
    query = """
        UPDATE import_batch_process 
        SET ib_status = %s, ib_end = NOW()
        WHERE ibp_id = %s;
    """

    with conn.cursor() as cur:
        cur.execute(query, (status, ibp_id))
        conn.commit()


def create_import_batch_process_task(conn, ibp_id, description, status):
    """Creates a new import batch process task and returns its ID."""
    query = """
        INSERT INTO import_batch_process_task (ibp_id, ib_description, ib_status, ib_start)
        VALUES (%s, %s, %s, NOW())
        RETURNING ibpt_id;
    """

    with conn.cursor() as cur:
        cur.execute(query, (ibp_id, description, status))
        ibpt_id = cur.fetchone()[0]
        conn.commit()

    return ibpt_id


def update_import_batch_process_task(
    conn,
    ibpt_id,
    status,
    records_in,
    records_failed,
    records_out,
    records_type_2,
    records_type_1,
    records_dim_new,
):
    """Updates the status, record counts, and end timestamp of an import batch process task."""
    query = """
        UPDATE import_batch_process_task
        SET ib_status = %s, ib_end = NOW(),
            ibpt_records_in = %s, ibpt_records_failed = %s, ibpt_records_out = %s,
            ibpt_records_type_2 = %s, ibpt_records_type_1 = %s, ibpt_records_dim_new = %s
        WHERE ibpt_id = %s;
    """

    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                status,
                records_in,
                records_failed,
                records_out,
                records_type_2,
                records_type_1,
                records_dim_new,
                ibpt_id,
            ),
        )
        conn.commit()


def record_failure(conn):
    """Finds the latest running batch, process, or task and marks it as 'Failed'."""

    queries = [
        # Update latest running process task
        """
        UPDATE import_batch_process_task
        SET ib_status = 'Failed', ib_end = NOW()
        WHERE ibpt_id = (
            SELECT ibpt_id FROM import_batch_process_task
            WHERE ib_status = 'Running'
            ORDER BY ib_start DESC
            LIMIT 1
        );
        """,
        # Update latest running process
        """
        UPDATE import_batch_process
        SET ib_status = 'Failed', ib_end = NOW()
        WHERE ibp_id = (
            SELECT ibp_id FROM import_batch_process
            WHERE ib_status = 'Running'
            ORDER BY ib_start DESC
            LIMIT 1
        );
        """,
        # Update latest running import batch
        """
        UPDATE import_batch
        SET ib_status = 'Failed', ib_end = NOW()
        WHERE ib_id = (
            SELECT ib_id FROM import_batch
            WHERE ib_status = 'Running'
            ORDER BY ib_start DESC
            LIMIT 1
        );
        """,
    ]

    with conn.cursor() as cur:
        for query in queries:
            cur.execute(query)
        conn.commit()

    print("Latest running import batch, process, and task marked as 'Failed'.")


def get_latest_running_import_batch(conn):
    """Retrieves the latest import batch ID with status 'Running'."""
    query = """
        SELECT ib_id 
        FROM import_batch
        WHERE ib_status = 'Running'
        ORDER BY ib_start DESC
        LIMIT 1;
    """

    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()
        return result[0] if result else None
