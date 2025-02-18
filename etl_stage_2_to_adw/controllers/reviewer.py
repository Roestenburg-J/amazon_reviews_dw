import psycopg2
from psycopg2 import sql
from utils.db_utills import connect_to_db, DB_STAGE2, DB_ADW, write_to_db
from utils.output_utils import write_failed_rows, convert_to_json
from utils.metadata_utils import (
    create_import_batch_process_task,
    update_import_batch_process_task,
    update_import_batch_process,
)

conn_stage_2 = connect_to_db(DB_STAGE2)
conn_adw = connect_to_db(DB_ADW)

adw_columns = ("reviewer_source_key", "reviewer_name")


def fetch_batch_from_stage_2(batch_size, offset):
    with conn_stage_2.cursor() as cursor:
        query = sql.SQL(
            """
            SELECT 
              DISTINCT
                r_reviewer_source_key,
                r_reviewer_name
            FROM v_s2_review
            LIMIT %s OFFSET %s
            """
        )
        cursor.execute(query, [batch_size, offset])
        return cursor.fetchall()


def update_reviewers_in_adw(cursor, reviewers_to_update):
    """Update reviewers in bulk."""
    update_query = sql.SQL(
        """
        UPDATE reviewer
        SET reviewer_name = %s
        WHERE reviewer_source_key = %s
        """
    )
    cursor.executemany(update_query, reviewers_to_update)


def insert_reviewers_into_adw(cursor, reviewers_to_insert):
    """Insert new reviewers in bulk."""
    insert_query = sql.SQL(
        """
        INSERT INTO reviewer (reviewer_source_key, reviewer_name)
        VALUES (%s, %s)
        """
    )
    cursor.executemany(insert_query, reviewers_to_insert)


def migrate_reviewer_to_adw(ibp_id):
    ibpt_id = create_import_batch_process_task(
        conn_adw, ibp_id, "adw_reviewer", "Running"
    )

    batch_size = 10000
    offset = 0

    records_in_count = 0
    records_failed_count = 0
    records_out_count = 0
    ibpt_records_type_1 = 0
    ibpt_records_dim_new = 0

    print("Migration to ADW starting...")

    try:
        while True:
            try:
                batch = fetch_batch_from_stage_2(batch_size, offset)
                records_in_count += len(batch)

                if not batch:
                    break

                reviewer_source_keys = [row[0] for row in batch]

                # Check which reviewers exist in ADW
                with conn_adw.cursor() as cursor:
                    # Dynamically create a query with placeholders for batch size
                    placeholders = sql.SQL(",").join(
                        [sql.Placeholder()] * len(reviewer_source_keys)
                    )
                    check_query = sql.SQL(
                        """
                        SELECT reviewer_source_key
                        FROM reviewer
                        WHERE reviewer_source_key IN ({})
                        """
                    ).format(placeholders)

                    cursor.execute(check_query, reviewer_source_keys)

                    existing_reviewers = {row[0] for row in cursor.fetchall()}

                    reviewers_to_update = []
                    reviewers_to_insert = []

                    for row in batch:
                        reviewer_source_key, reviewer_name = row
                        if reviewer_source_key in existing_reviewers:
                            reviewers_to_update.append(
                                (reviewer_name, reviewer_source_key)
                            )
                        else:
                            reviewers_to_insert.append(
                                (reviewer_source_key, reviewer_name)
                            )

                    ibpt_records_type_1 += len(reviewers_to_update)
                    ibpt_records_dim_new += len(reviewers_to_update)

                    # Perform the bulk update and insert operations
                    if reviewers_to_update:
                        update_reviewers_in_adw(cursor, reviewers_to_update)
                    if reviewers_to_insert:
                        insert_reviewers_into_adw(cursor, reviewers_to_insert)

                    conn_adw.commit()

                records_out_count += len(batch)

            except Exception as e:
                print(f"Error during migration to ADW: {e}")

                # write_failed_rows(
                #     "./logs/adw_review_failed_records.json",
                #     convert_to_json(batch),
                # )
                records_failed_count += len(batch)
                write_failed_rows(
                    "./logs/adw_review_error_logs.json",
                    [{"batch_error": str(e), "offset": offset, "entity": "adw_review"}],
                )

            offset += batch_size

        update_import_batch_process_task(
            conn_adw,
            ibpt_id,
            "Completed",
            records_in_count,
            records_failed_count,
            records_out_count,
            None,
            ibpt_records_type_1,
            ibpt_records_dim_new,
        )
        print("Migration to ADW complete.")

    except Exception as e:
        print(f"Error during migration: {e}")
        update_import_batch_process_task(
            conn_adw,
            ibpt_id,
            "Failed",
            records_in_count,
            records_failed_count,
            records_out_count,
            None,
            ibpt_records_type_1,
            ibpt_records_dim_new,
        )
        update_import_batch_process(conn_adw, ibp_id, "Failed")

    # Close connections
    finally:
        conn_stage_2.close()
        conn_adw.close()
