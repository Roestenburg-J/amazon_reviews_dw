import psycopg2
from psycopg2 import sql
from utils.db_utills import connect_to_db, DB_STAGE2, DB_ADW, write_to_db
from utils.output_utils import write_failed_rows
from utils.metadata_utils import (
    create_import_batch_process_task,
    update_import_batch_process_task,
    update_import_batch_process,
)

conn_stage_2 = connect_to_db(DB_STAGE2)
conn_adw = connect_to_db(DB_ADW)

adw_columns = (
    "date_reviewed_key",
    "reviewer_key",
    "product_key",
    "review_descriptors_key",
    "helpfulness_rating",
    "review_rating",
)


# Step 1: Fetch the batch of rows from the fact table (review_fact)
def fetch_batch_from_stage_2(batch_size, offset):
    with conn_stage_2.cursor() as cursor:
        query = sql.SQL(
            """
            SELECT 
                r_review_date_key, 
                r_reviewer_source_key, 
                r_product_key,  
                r_helpfulness_rating, 
                r_review_score,
                r_review_title,
                r_review_text
            FROM v_s2_review
            LIMIT %s OFFSET %s
            """
        )
        cursor.execute(query, [batch_size, offset])
        return cursor.fetchall()


def fetch_reviewer_keys(cursor, reviewer_ids, chunk_size=5000):
    all_results = []
    for i in range(0, len(reviewer_ids), chunk_size):
        chunk = reviewer_ids[i : i + chunk_size]
        placeholders = ",".join(
            ["%s"] * len(chunk)
        )  # Create placeholders for the chunk
        query = sql.SQL(
            """
            SELECT reviewer_source_key, reviewer_key
            FROM reviewer
            WHERE reviewer_source_key IN ({})
            """
        ).format(sql.SQL(placeholders))

        cursor.execute(query, chunk)
        all_results.extend(cursor.fetchall())  # Collect results from each chunk

    return all_results


def fetch_review_descriptor_keys(cursor, review_descriptor_pairs, chunk_size=5000):
    all_results = []

    for i in range(0, len(review_descriptor_pairs), chunk_size):
        chunk = review_descriptor_pairs[i : i + chunk_size]
        placeholders = ",".join(
            [f"(%s, %s)"] * len(chunk)
        )  # Create placeholders for the chunk
        flattened_chunk = [item for pair in chunk for item in pair]  # Flatten the pairs

        query = sql.SQL(
            """
            SELECT review_text, review_title, review_descriptors_key
            FROM review_descriptors
            WHERE (review_text, review_title) IN ({})
            """
        ).format(sql.SQL(placeholders))

        cursor.execute(query, flattened_chunk)
        all_results.extend(cursor.fetchall())  # Collect results from each chunk

    return all_results


def fetch_product_keys(cursor, product_ids, chunk_size=5000):
    all_results = []

    for i in range(0, len(product_ids), chunk_size):
        chunk = product_ids[i : i + chunk_size]
        placeholders = ",".join(
            ["%s"] * len(chunk)
        )  # Create placeholders for the chunk

        query = sql.SQL(
            """
            SELECT product_source_key, product_key
            FROM product
            WHERE product_source_key IN ({}) AND is_current = TRUE
            """
        ).format(sql.SQL(placeholders))

        cursor.execute(query, chunk)
        all_results.extend(cursor.fetchall())  # Collect results from each chunk

    return all_results


def migrate_fact_table_to_adw(ibp_id):
    ibpt_id = create_import_batch_process_task(
        conn_adw, ibp_id, "adw_review_fact", "Running"
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

                # Prepare lists to fetch surrogate keys for each dimension
                reviewer_ids = [row[1] for row in batch]
                product_ids = [row[2] for row in batch]
                review_descriptor_pairs = [(row[5], row[6]) for row in batch]

                with conn_adw.cursor() as cursor:
                    # Step 2: Fetch surrogate keys for reviewer, product, and review_descriptors
                    reviewer_keys = fetch_reviewer_keys(cursor, reviewer_ids)
                    product_keys = fetch_product_keys(cursor, product_ids)
                    review_descriptor_keys = fetch_review_descriptor_keys(
                        cursor, review_descriptor_pairs
                    )

                    # Create mappings for dimensions (excluding date)
                    reviewer_map = {
                        row[0]: row[1]
                        for row in reviewer_keys
                        if row[0] in reviewer_ids
                    }
                    product_map = {
                        row[0]: row[1] for row in product_keys if row[0] in product_ids
                    }
                    review_descriptor_map = {
                        (row[0], row[1]): row[2]
                        for row in review_descriptor_keys
                        if (row[0], row[1]) in review_descriptor_pairs
                    }

                    # Prepare the fact data with surrogate keys
                    records_to_insert = []

                    for row in batch:
                        (
                            date_reviewed_key,  # r_review_date_key is used directly
                            reviewer_key,
                            product_key,
                            helpfulness_rating,
                            review_rating,
                            review_title,
                            review_text,
                        ) = row  # Unpack 7 values

                        # Fetch surrogate keys
                        new_reviewer_key = reviewer_map.get(reviewer_key)
                        new_product_key = product_map.get(product_key)
                        new_review_descriptors_key = review_descriptor_map.get(
                            (review_title, review_text)
                        )

                        # If valid surrogate keys are found, add to records to insert
                        if (
                            new_reviewer_key
                            and new_product_key
                            and new_review_descriptors_key
                        ):
                            records_to_insert.append(
                                (
                                    date_reviewed_key,  # Use date_reviewed_key directly
                                    new_reviewer_key,
                                    new_product_key,
                                    new_review_descriptors_key,
                                    helpfulness_rating,
                                    review_rating,
                                )
                            )

                    # Step 6: Insert records
                    if records_to_insert:
                        write_to_db(
                            cursor,
                            "review_fact",
                            adw_columns,
                            records_to_insert,
                        )
                        conn_adw.commit()

                records_out_count += len(records_to_insert)

            except Exception as e:
                print(f"Error during migration to ADW: {e}")
                records_failed_count += len(batch)
                write_failed_rows(
                    "./logs/adw_review_fact_error_logs.json",
                    [
                        {
                            "batch_error": str(e),
                            "offset": offset,
                            "entity": "adw_review_fact",
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
