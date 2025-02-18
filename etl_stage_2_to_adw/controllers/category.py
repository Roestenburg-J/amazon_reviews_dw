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

adw_columns = ("product_category",)


def fetch_batch_from_stage_2(batch_size, offset):
    with conn_stage_2.cursor() as cursor:
        query = sql.SQL(
            """
            SELECT DISTINCT
                pc_category          
            FROM v_s2_product_categories_only
            LIMIT %s OFFSET %s
            """
        )
        cursor.execute(query, [batch_size, offset])
        return cursor.fetchall()


def check_if_categories_exist(cursor, categories_to_check):

    placeholders = [sql.Placeholder()] * len(categories_to_check)

    check_query = sql.SQL(
        """
        SELECT product_category
        FROM category
        WHERE product_category IN ({})
        """
    ).format(sql.SQL(",").join(placeholders))

    cursor.execute(check_query, categories_to_check)
    return cursor.fetchall()


def migrate_category_to_adw(ibp_id):
    ibpt_id = create_import_batch_process_task(
        conn_adw, ibp_id, "adw_category", "Running"
    )

    batch_size = 100000
    offset = 0

    records_in_count = 0
    records_failed_count = 0
    records_out_count = 0

    print("Migration to ADW starting...")

    try:
        while True:
            try:
                batch = fetch_batch_from_stage_2(batch_size, offset)
                records_in_count += len(batch)

                if not batch:
                    break

                categories_to_check = [row[0] for row in batch]

                with conn_adw.cursor() as cursor:
                    existing_categories = check_if_categories_exist(
                        cursor, categories_to_check
                    )

                    existing_categories_set = set(row[0] for row in existing_categories)

                    records_to_insert = []

                    for row in batch:
                        category = row
                        if (category,) not in existing_categories_set:

                            records_to_insert.append((category))

                    if records_to_insert:
                        write_to_db(cursor, "category", adw_columns, records_to_insert)
                        conn_adw.commit()

                records_out_count += len(records_to_insert)

            except Exception as e:
                print(f"Error during migration to ADW: {e}")
                # write_failed_rows(
                #     "./logs/adw_category_failed_records.json",
                #     convert_to_json(batch),
                # )
                records_failed_count += len(batch)
                write_failed_rows(
                    "./logs/adw_category_error_logs.json",
                    [
                        {
                            "batch_error": str(e),
                            "offset": offset,
                            "entity": "adw_category",
                        }
                    ],
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
            None,
            records_out_count,
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
            None,
            records_out_count,
        )
        update_import_batch_process(conn_adw, ibp_id, "Failed")

    # Close connections
    finally:
        conn_stage_2.close()
        conn_adw.close()
