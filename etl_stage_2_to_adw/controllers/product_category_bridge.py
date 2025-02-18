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

adw_columns = ("product_key", "category_key")


def fetch_batch_from_stage_2(batch_size, offset):
    with conn_stage_2.cursor() as cursor:
        query = sql.SQL(
            """
            SELECT 
                pc_product_source_key, 
                pc_category
            FROM v_s2_product_category
            LIMIT %s OFFSET %s
            """
        )
        cursor.execute(query, [batch_size, offset])
        return cursor.fetchall()


def fetch_category_keys_by_names(cursor, category_names):
    query = sql.SQL(
        """
        SELECT product_category, category_key
        FROM category
        WHERE product_category IN ({})
        """
    ).format(sql.SQL(",").join(sql.Placeholder() for _ in category_names))

    cursor.execute(query, category_names)

    return cursor.fetchall()


def fetch_product_keys_by_source_keys(cursor, product_source_keys):
    """
    Fetch product_key values for a list of product_source_keys.
    """
    query = sql.SQL(
        """
        SELECT product_source_key, product_key
        FROM product
        WHERE product_source_key IN ({})
        """
    ).format(sql.SQL(",").join(sql.Placeholder() for _ in product_source_keys))

    cursor.execute(query, product_source_keys)
    return cursor.fetchall()


def check_if_combinations_exist(cursor, combinations_to_check):
    if not combinations_to_check:
        return []  # Return early if no combinations to check

    # Create a direct query for checking existing combinations
    query = """
        SELECT pcb.product_key, pcb.category_key
        FROM product_category_bridge pcb
        WHERE (pcb.product_key, pcb.category_key) IN %s
    """

    # Execute the query with combinations
    cursor.execute(query, (tuple(combinations_to_check),))

    return cursor.fetchall()


def migrate_product_category_bridge_to_adw(ibp_id):
    ibpt_id = create_import_batch_process_task(
        conn_adw, ibp_id, "adw_product_category_bridge", "Running"
    )

    batch_size = 10000
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

                # Extract unique product_source_keys and category_names
                product_source_keys = list(set(row[0] for row in batch))
                category_names = list(set(row[1] for row in batch))

                with conn_adw.cursor() as cursor:
                    # Fetch product_keys
                    product_keys = fetch_product_keys_by_source_keys(
                        cursor, product_source_keys
                    )
                    product_source_to_key = {psk: pk for psk, pk in product_keys}

                    # Fetch category_keys
                    category_keys = fetch_category_keys_by_names(cursor, category_names)
                    category_name_to_key = {
                        category_name: category_key
                        for category_name, category_key in category_keys
                    }

                    # Prepare combinations to check
                    combinations_to_check = []
                    for product_source_key, category_name in batch:
                        product_key = product_source_to_key.get(product_source_key)
                        category_key = category_name_to_key.get(category_name)

                        if product_key and category_key:
                            combinations_to_check.append((product_key, category_key))

                    # Check if these combinations exist in product_category_bridge
                    existing_combinations = check_if_combinations_exist(
                        cursor, combinations_to_check
                    )
                    existing_combinations_set = set(
                        (row[0], row[1]) for row in existing_combinations
                    )

                    # Filter out already existing records
                    records_to_insert = [
                        (product_key, category_key)
                        for product_key, category_key in combinations_to_check
                        if (product_key, category_key) not in existing_combinations_set
                    ]

                    # Insert new records
                    if records_to_insert:
                        write_to_db(
                            cursor,
                            "product_category_bridge",
                            adw_columns,
                            records_to_insert,
                        )
                        conn_adw.commit()

                records_out_count += len(records_to_insert)

            except Exception as e:
                print(f"Error during migration to ADW: {e}")
                records_failed_count += len(batch)
                write_failed_rows(
                    "./logs/adw_product_category_bridge_error_logs.json",
                    [
                        {
                            "batch_error": str(e),
                            "offset": offset,
                            "entity": "adw_product_category_bridge",
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

    finally:
        conn_stage_2.close()
        conn_adw.close()
