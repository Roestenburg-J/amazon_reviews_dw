import psycopg2
import io
from psycopg2 import sql

from utils.db_utills import connect_to_db, DB_STAGE1, DB_STAGE2, DB_ADW, write_to_db
from utils.output_utils import write_failed_rows, convert_to_json
from utils.metadata_utils import (
    create_import_batch_process_task,
    update_import_batch_process_task,
    update_import_batch_process,
)

conn_stage_1 = connect_to_db(DB_STAGE1)
conn_stage_2 = connect_to_db(DB_STAGE2)
conn_meta = connect_to_db(DB_ADW)


stage_2_columns = (
    "r_reviewer_source_key",
    "r_product_key",
    "r_reviewer_name",
    "r_helpfulness_rating",
    "r_review_text",
    "r_review_score",
    "r_review_title",
    "r_review_datetime",
)


def fetch_batch(batch_size, offset):
    with conn_stage_1.cursor() as cursor:
        query = sql.SQL(
            """
            SELECT 
                r_reviewer_source_key,
                r_product_key,
                r_reviewer_name,
                r_helpfulness_rating,
                r_review_text,
                r_review_score,
                r_review_title,
                r_review_datetime
              FROM v_s1_review LIMIT %s OFFSET %s
              """
        )
        cursor.execute(query, [batch_size, offset])
        return cursor.fetchall()


def migrate_reviews(ibp_id):

    ibpt_id = create_import_batch_process_task(
        conn_meta, ibp_id, "s2_review", "Running"
    )

    batch_size = 10000
    offset = 0

    records_in_count = 0
    records_failed_count = 0
    records_out_count = 0

    print("Review migration starting...")

    try:

        while True:

            try:
                batch = fetch_batch(batch_size, offset)
                records_in_count += len(batch)

                if not batch:
                    break

                with conn_stage_2.cursor() as cursor:
                    write_to_db(cursor, "s2_review", stage_2_columns, batch)
                    conn_stage_2.commit()

                records_out_count += len(batch)

            except Exception as e:
                print(f"Error during review batch migration: {e}")

                write_failed_rows(
                    "./logs/s2_review_failed_records.json",
                    convert_to_json(batch),
                )
                records_failed_count += len(batch)
                write_failed_rows(
                    "./logs/s2_review_error_logs.json",
                    [{"batch_error": str(e), "offset": offset, "entity": "s2_review"}],
                )

            offset += batch_size

        update_import_batch_process_task(
            conn_meta,
            ibpt_id,
            "Completed",
            records_in_count,
            records_failed_count,
            records_out_count,
            None,
            None,
            None,
        )
        print("Review migration complete.")

    except Exception as e:
        print(f"Error during review migration: {e}")
        update_import_batch_process_task(
            conn_meta,
            ibpt_id,
            "Failed",
            records_in_count,
            records_failed_count,
            records_out_count,
            None,
            None,
            None,
        )
        update_import_batch_process(conn_meta, ibp_id, "Failed")

    finally:
        conn_stage_1.close()
        conn_stage_2.close()
        conn_meta.close()
