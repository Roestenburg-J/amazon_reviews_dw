import psycopg2
import psycopg2.extras  # Import extras
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

adw_columns = ("primary_product_key", "secondary_product_key", "relation")


def fetch_batch_from_stage_2(batch_size, offset):
    with conn_stage_2.cursor() as cursor:
        query = sql.SQL(
            """
            SELECT 
                rl_product_source_key,
                rl_related_product_source_key,
                rl_relation
            FROM v_s2_related_product
            LIMIT %s OFFSET %s
            """
        )
        cursor.execute(query, [batch_size, offset])
        return cursor.fetchall()


def fetch_product_keys(cursor, product_source_keys):
    query = sql.SQL(
        """
        SELECT product_source_key, product_key
        FROM product
        WHERE product_source_key IN ({})
        """
    ).format(sql.SQL(",").join(sql.Placeholder() for _ in product_source_keys))

    cursor.execute(query, product_source_keys)
    return cursor.fetchall()


def check_if_relations_exist(cursor, relations_to_check, batch_size=10000):
    if not relations_to_check:
        return []

    # Split relations into smaller batches
    relation_batches = [
        relations_to_check[i : i + batch_size]
        for i in range(0, len(relations_to_check), batch_size)
    ]

    all_existing_relations = []

    for batch in relation_batches:
        # Construct the query dynamically based on the batch size
        placeholders = ",".join(["%s"] * len(batch))
        check_query = sql.SQL(
            """
            SELECT primary_product_key, secondary_product_key, relation
            FROM related_product
            WHERE (primary_product_key, secondary_product_key, relation) IN ({})
            """
        ).format(sql.SQL(placeholders))

        cursor.execute(check_query, batch)
        all_existing_relations.extend(cursor.fetchall())

    return all_existing_relations


def migrate_related_product_to_adw(ibp_id):
    ibpt_id = create_import_batch_process_task(
        conn_adw, ibp_id, "adw_related_product", "Running"
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

                # Prepare a list of product source keys from the batch
                product_source_keys = list(
                    set(row[0] for row in batch) | set(row[1] for row in batch)
                )

                # Perform batch query to get the product_key for all unique product_source_keys
                with conn_adw.cursor() as cursor:
                    product_key_map = dict(
                        fetch_product_keys(cursor, product_source_keys)
                    )

                    # Prepare a list of relations to check
                    relations_to_insert = []
                    for row in batch:
                        (
                            primary_product_source_key,
                            secondary_product_source_key,
                            relation,
                        ) = row
                        primary_product_key = product_key_map.get(
                            primary_product_source_key
                        )
                        secondary_product_key = product_key_map.get(
                            secondary_product_source_key
                        )

                        if primary_product_key and secondary_product_key:
                            relations_to_insert.append(
                                (primary_product_key, secondary_product_key, relation)
                            )

                    # Insert records that do not already exist, using ON CONFLICT to avoid duplicates
                    if relations_to_insert:
                        insert_query = sql.SQL(
                            """
                            INSERT INTO related_product (primary_product_key, secondary_product_key, relation)
                            VALUES %s
                            ON CONFLICT (primary_product_key, secondary_product_key) DO NOTHING
                            """
                        )

                        psycopg2.extras.execute_values(
                            cursor, insert_query, relations_to_insert
                        )
                        conn_adw.commit()

                records_out_count += len(relations_to_insert)

            except Exception as e:
                print(f"Error during migration to ADW: {e}")
                records_failed_count += len(batch)
                write_failed_rows(
                    "./logs/adw_related_product_error_logs.json",
                    [
                        {
                            "batch_error": str(e),
                            "offset": offset,
                            "entity": "adw_related_product",
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
