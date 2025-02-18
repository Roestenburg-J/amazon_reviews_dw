import csv
from itertools import islice

from utils.db_utills import connect_to_db, write_to_db, DB_ADW
from utils.data_utils import sanitize_string, convert_value
from utils.output_utils import write_failed_rows
from utils.metadata_utils import (
    create_import_batch_process_task,
    update_import_batch_process_task,
    update_import_batch_process,
)

# Max lengths for columns
MAX_LENGTHS = {
    "p_product_metadata_id": 7,
    "p_product_source_key": 10,
    "p_sales_rank_category": 50,
    "p_image_url": 225,
    # "p_title": 100,
    # "p_description": 225,
    "p_brand": 150,
    "pc_product_source_key": 10,
    "pc_category": 150,
    "rl_product_source_key": 10,
    "rl_related_product_source_key": 10,
    "rl_relation": 20,
}


# Process: take 1000 rows, transform each column value in each row
# Write transformed rows to tables in S1
# Log rows that were invalid
def ingest_products(csvFilePath, ibp_id):

    conn_meta = connect_to_db(DB_ADW)
    conn = connect_to_db()

    ibpt_id_products = create_import_batch_process_task(
        conn_meta, ibp_id, "s1_product", "Running"
    )

    ibpt_id_product_categories = create_import_batch_process_task(
        conn_meta, ibp_id, "s1_product_category", "Running"
    )

    ibpt_id_related_products = create_import_batch_process_task(
        conn_meta, ibp_id, "s1_related_products", "Running"
    )

    chunk_size = 1000  # Process in chunks of 1000
    chunk_count = 0

    records_in_count = 0
    records_failed_count_p = 0
    records_out_count_p = 0

    records_failed_count_pc = 0
    records_out_count_pc = 0

    records_failed_count_rp = 0
    records_out_count_rp = 0

    try:

        print("Product ingestion starting...")

        with open(csvFilePath, encoding="utf-8") as csvf:
            cursor = conn.cursor()
            csvReader = csv.DictReader(csvf)

            while True:
                chunk = list(islice(csvReader, chunk_size))
                records_in_count += len(chunk)
                if not chunk:
                    break

                transformed_products = []
                transformed_categories = []
                transformed_related_products = []
                failed_rows_p = []
                failed_rows_pc = []
                failed_rows_rp = []

                error_logs = []

                for row in chunk:
                    try:
                        # First select fields for product table, and transform
                        product_metadata_id = str(row["metadataid"])
                        product_source_key = str(row["asin"])

                        try:
                            salesrank = convert_value(row["salesrank"])
                            if isinstance(salesrank, dict):

                                sales_rank_category, sales_rank = next(
                                    iter(salesrank.items())
                                )
                            else:
                                sales_rank_category = "*Unknown category"
                                sales_rank = -1
                        except Exception as e:
                            sales_rank_category = "*Unknown category"
                            sales_rank = -1

                        image_url = (
                            str(row["imurl"]) if row["imurl"] else "*Unknown URL"
                        )
                        title = (
                            sanitize_string(str(row["title"]))
                            if row["title"]
                            else "*Unknown title"
                        )
                        description = (
                            sanitize_string(str(row["description"]))
                            if row["description"]
                            else "*Unknown description"
                        )
                        price = float(row["price"]) if row["price"] else -1.00
                        brand = (
                            sanitize_string(str(row["brand"]))
                            if row["brand"]
                            else "*Unknown brand"
                        )

                        if (
                            len(product_metadata_id)
                            > MAX_LENGTHS["p_product_metadata_id"]
                        ):
                            failed_rows_p.append(
                                {
                                    "row": row,
                                    "error": "product_metadata_id too long",
                                    "ibpt_id": ibpt_id_products,
                                }
                            )
                            continue
                        if (
                            len(product_source_key)
                            > MAX_LENGTHS["p_product_source_key"]
                        ):
                            failed_rows_p.append(
                                {
                                    "row": row,
                                    "error": "product_source_key too long",
                                    "ibpt_id": ibpt_id_products,
                                }
                            )
                            continue
                        if (
                            len(sales_rank_category)
                            > MAX_LENGTHS["p_sales_rank_category"]
                        ):
                            failed_rows_p.append(
                                {
                                    "row": row,
                                    "error": "sales_rank_category too long",
                                    "ibpt_id": ibpt_id_products,
                                }
                            )
                            continue
                        if len(image_url) > MAX_LENGTHS["p_image_url"]:
                            failed_rows_p.append(
                                {
                                    "row": row,
                                    "error": "image_url too long",
                                    "ibpt_id": ibpt_id_products,
                                }
                            )
                            continue
                        if len(brand) > MAX_LENGTHS["p_brand"]:
                            failed_rows_p.append(
                                {
                                    "row": row,
                                    "error": "brand too long",
                                    "ibpt_id": ibpt_id_products,
                                }
                            )
                            continue

                        transformed_products.append(
                            [
                                product_metadata_id,
                                product_source_key,
                                sales_rank_category,
                                sales_rank,
                                image_url,
                                title,
                                description,
                                price,
                                brand,
                            ]
                        )

                        # Flatten categories and transform.
                        categories = convert_value(row["categories"])
                        valid_categories = []

                        def flatten_categories(categories):
                            """Helper function to flatten any nested arrays within categories"""
                            flattened = []
                            for item in categories:
                                if isinstance(
                                    item, list
                                ):  # If the item is a list, recursively flatten it
                                    flattened.extend(
                                        flatten_categories(item)
                                    )  # Unroll nested arrays
                                else:
                                    flattened.append(
                                        item
                                    )  # Otherwise, just add the item
                            return flattened

                        if isinstance(categories, list):
                            flat_categories = flatten_categories(categories)

                            for category in flat_categories:
                                if len(category) <= MAX_LENGTHS["pc_category"]:
                                    valid_categories.append(
                                        [product_source_key, category]
                                    )
                                else:
                                    failed_rows_pc.append(
                                        {
                                            "row": row,
                                            "error": "category too long",
                                            "ibpt_id": ibpt_id_product_categories,
                                        }
                                    )
                            transformed_categories.extend(valid_categories)
                        else:
                            failed_rows_pc.append(
                                {
                                    "row": row,
                                    "error": "invalid categories format",
                                    "ibpt_id": ibpt_id_product_categories,
                                }
                            )
                            continue

                        # Flatten related products, extract relation
                        related_products = convert_value(row["related"])
                        if isinstance(related_products, dict):
                            valid_related_products = []

                            for relation_type, products in related_products.items():
                                for related_product in products:
                                    related_product_source_key = str(related_product)
                                    relation = str(relation_type)
                                    if (
                                        len(related_product_source_key)
                                        <= MAX_LENGTHS["rl_product_source_key"]
                                        and len(relation) <= MAX_LENGTHS["rl_relation"]
                                    ):
                                        valid_related_products.append(
                                            [
                                                product_source_key,
                                                related_product_source_key,
                                                relation,
                                            ]
                                        )
                                    else:
                                        failed_rows_rp.append(
                                            {
                                                "row": row,
                                                "error": "related product fields too long",
                                                "ibpt_id": ibpt_id_related_products,
                                            }
                                        )

                        transformed_related_products.extend(valid_related_products)

                    except Exception as e:
                        failed_rows_p.append({"row": row, "error": str(e)})
                        continue

                # Write transformed products to DB
                if transformed_products:
                    try:
                        write_to_db(
                            cursor,
                            "s1_product",
                            [
                                "p_product_metadata_id",
                                "p_product_source_key",
                                "p_sales_rank_category",
                                "p_sales_rank",
                                "p_image_url",
                                "p_title",
                                "p_description",
                                "p_price",
                                "p_brand",
                            ],
                            transformed_products,
                        )
                        conn.commit()
                        records_out_count_p += len(transformed_products)

                    except Exception as e:
                        error_logs.append(
                            {
                                "chunk_error": str(e),
                                "chunk": chunk_count,
                                "entity": "products",
                            }
                        )

                # Write transformed categories to DB
                if transformed_categories:
                    try:
                        write_to_db(
                            cursor,
                            "s1_product_category",
                            ["pc_product_source_key", "pc_category"],
                            transformed_categories,
                        )
                        conn.commit()
                        records_out_count_pc += len(transformed_categories)

                    except Exception as e:
                        error_logs.append(
                            {
                                "chunk_error": str(e),
                                "chunk": chunk_count,
                                "entity": "categories",
                            }
                        )

                    # with open(
                    #     "pc_output.csv", "w", newline="", encoding="utf-8"
                    # ) as file:
                    #     writer = csv.writer(file)
                    #     writer.writerows(
                    #         transformed_categories
                    #     )
                    # break

                # Write related products to DB
                if transformed_related_products:
                    try:
                        write_to_db(
                            cursor,
                            "s1_related_product",
                            [
                                "rl_product_source_key",
                                "rl_related_product_source_key",
                                "rl_relation",
                            ],
                            transformed_related_products,
                        )
                        conn.commit()
                        records_out_count_rp += len(transformed_related_products)

                    except Exception as e:
                        error_logs.append(
                            {
                                "chunk_error": str(e),
                                "chunk": chunk_count,
                                "entity": "retaled_products",
                            }
                        )

                if failed_rows_p:
                    write_failed_rows("./logs/product_failed_rows.json", failed_rows_pc)
                    records_failed_count_p += len(failed_rows_p)
                if failed_rows_pc:
                    write_failed_rows(
                        "./logs/product_categories_failed_rows.json", failed_rows_pc
                    )
                    records_failed_count_pc += len(failed_rows_pc)

                if failed_rows_rp:
                    write_failed_rows(
                        "./logs/related_products_failed_rows.json", failed_rows_pc
                    )
                    records_failed_count_rp += len(failed_rows_rp)

                if error_logs:
                    write_failed_rows("./logs/product_error_logs.json", error_logs)

                chunk_count += 1

        update_import_batch_process_task(
            conn_meta,
            ibpt_id_products,
            "Completed",
            records_in_count,
            records_failed_count_p,
            records_out_count_p,
            None,
            None,
            None,
        )

        update_import_batch_process_task(
            conn_meta,
            ibpt_id_product_categories,
            "Completed",
            records_in_count,
            records_failed_count_pc,
            records_out_count_pc,
            None,
            None,
            None,
        )

        update_import_batch_process_task(
            conn_meta,
            ibpt_id_related_products,
            "Completed",
            records_in_count,
            records_failed_count_rp,
            records_out_count_rp,
            None,
            None,
            None,
        )

        print("Product ingestion complete.")

    except KeyboardInterrupt:
        update_import_batch_process_task(
            conn_meta,
            ibpt_id_products,
            "Aborted",
            records_in_count,
            records_failed_count_p,
            records_out_count_p,
            None,
            None,
            None,
        )

        update_import_batch_process_task(
            conn_meta,
            ibpt_id_product_categories,
            "Aborted",
            records_in_count,
            records_failed_count_pc,
            records_out_count_pc,
            None,
            None,
            None,
        )

        update_import_batch_process_task(
            conn_meta,
            ibpt_id_related_products,
            "Aborted",
            records_in_count,
            records_failed_count_rp,
            records_out_count_rp,
            None,
            None,
            None,
        )

        update_import_batch_process(conn, ibp_id, "Aborted")

    except Exception as e:
        print(f"Error during product ingestion: {e}")
        update_import_batch_process_task(
            conn_meta,
            ibpt_id_products,
            "Failed",
            records_in_count,
            records_failed_count_p,
            records_out_count_p,
            None,
            None,
            None,
        )

        update_import_batch_process_task(
            conn_meta,
            ibpt_id_product_categories,
            "Failed",
            records_in_count,
            records_failed_count_pc,
            records_out_count_pc,
            None,
            None,
            None,
        )

        update_import_batch_process_task(
            conn_meta,
            ibpt_id_related_products,
            "Failed",
            records_in_count,
            records_failed_count_rp,
            records_out_count_rp,
            None,
            None,
            None,
        )

        update_import_batch_process(conn, ibp_id, "Failed")

    finally:
        conn_meta.close()
        conn.close()
