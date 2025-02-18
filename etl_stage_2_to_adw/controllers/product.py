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

adw_columns = (
    "product_source_key",
    "product_metadata_id",
    "sales_rank_category",
    "sales_rank",
    "product_image_url",
    "product_title",
    "product_description",
    "price",
    "brand",
)


# Fetch batch of records from the source
def fetch_batch_from_stage_2(batch_size, offset):
    with conn_stage_2.cursor() as cursor:
        query = sql.SQL(
            """
            SELECT 
                p_product_source_key,
                p_product_metadata_id,
                p_sales_rank_category,
                p_sales_rank,
                p_image_url,
                p_title,
                p_description,
                p_price,
                p_brand
            FROM v_s2_product
            LIMIT %s OFFSET %s
            """
        )
        cursor.execute(query, [batch_size, offset])
        return cursor.fetchall()


# Function to insert new products in bulk
def insert_product_scd2(cursor, product_details_list):
    insert_query = sql.SQL(
        """
        INSERT INTO product (
            product_source_key, product_metadata_id, sales_rank_category, sales_rank, 
            product_image_url, product_title, product_description, price, brand, 
            effective_date, is_current
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, TRUE)
        """
    )
    cursor.executemany(insert_query, product_details_list)


# Function to update existing products in bulk
def update_product_scd2(cursor, product_source_key_list, product_details_list):
    # Expire the current records
    update_query = sql.SQL(
        """
        UPDATE product
        SET expiration_date = CURRENT_TIMESTAMP, is_current = FALSE
        WHERE product_source_key = ANY(%s) AND is_current = TRUE
        """
    )
    cursor.execute(update_query, [product_source_key_list])

    # Insert new records with updated information
    insert_product_scd2(cursor, product_details_list)


# Migration to ADW
def migrate_product_to_adw(ibp_id):
    ibpt_id = create_import_batch_process_task(
        conn_adw, ibp_id, "adw_product", "Running"
    )

    batch_size = 10000
    offset = 0

    records_in_count = 0
    records_failed_count = 0
    records_out_count = 0
    ibpt_records_type_2 = 0
    ibpt_records_dim_new = 0

    print("Migration to ADW starting...")

    try:
        while True:
            try:
                # Fetch batch from stage_2
                batch = fetch_batch_from_stage_2(batch_size, offset)
                records_in_count += len(batch)

                if not batch:
                    break

                product_source_keys = [row[0] for row in batch]

                # Check which products exist in ADW
                with conn_adw.cursor() as cursor:
                    # Fetch existing product keys from ADW
                    check_query = sql.SQL(
                        """
                        SELECT product_source_key
                        FROM product
                        WHERE product_source_key IN ({})
                        AND is_current = TRUE
                        """
                    ).format(
                        sql.SQL(",").join(
                            [sql.Placeholder()] * len(product_source_keys)
                        )  # Correctly formatting the placeholders
                    )
                    cursor.execute(check_query, product_source_keys)

                    existing_products = {row[0] for row in cursor.fetchall()}

                    products_to_update = []
                    products_to_insert = []
                    product_source_key_list = []

                    for row in batch:
                        (
                            product_source_key,
                            product_metadata_id,
                            sales_rank_category,
                            sales_rank,
                            product_image_url,
                            product_title,
                            product_description,
                            price,
                            brand,
                        ) = row

                        # Compare and decide if the product needs to be inserted or updated
                        product_details = (
                            product_source_key,
                            product_metadata_id,
                            sales_rank_category,
                            sales_rank,
                            product_image_url,
                            product_title,
                            product_description,
                            price,
                            brand,
                        )

                        if product_source_key in existing_products:
                            # If product exists, update it using SCD2
                            products_to_update.append(product_details)
                            product_source_key_list.append(product_source_key)
                            ibpt_records_type_2 += 1
                        else:
                            # If product doesn't exist, insert it
                            products_to_insert.append(product_details)
                            ibpt_records_dim_new += 1

                    # Perform bulk updates and inserts
                    with conn_adw.cursor() as cursor:
                        if products_to_update:
                            update_product_scd2(
                                cursor, product_source_key_list, products_to_update
                            )
                        if products_to_insert:
                            insert_product_scd2(cursor, products_to_insert)

                        # Commit the transaction for batch
                        conn_adw.commit()

                records_out_count += len(batch)

            except Exception as e:
                print(f"Error during migration to ADW: {e}")
                # write_failed_rows(
                #     "./logs/adw_product_failed_records.json", convert_to_json(batch)
                # )
                records_failed_count += len(batch)
                write_failed_rows(
                    "./logs/adw_product_error_logs.json",
                    [
                        {
                            "batch_error": str(e),
                            "offset": offset,
                            "entity": "adw_product",
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
            ibpt_records_type_2,
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
            ibpt_records_type_2,
            ibpt_records_dim_new,
        )
        update_import_batch_process(conn_adw, ibp_id, "Failed")

    # Close connections
    finally:
        conn_stage_2.close()
        conn_adw.close()
