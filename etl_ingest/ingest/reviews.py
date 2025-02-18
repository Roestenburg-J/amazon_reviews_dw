import csv
from datetime import datetime
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
    "r_reviewer_source_key": 21,
    "r_product_key": 10,
    # "r_reviewer_name": 100,
    # "r_review_text": 225,
    # "r_review_title": 150,
    "r_review_dateTime": 19,
}


# Process: take 1000 rows, transform each column value in each row
# Write transformed rows to tables in S1
# Log rows that were invalid
def ingest_reviews(csvFilePath, ibp_id):

    conn_meta = connect_to_db(DB_ADW)
    conn = connect_to_db()

    ibpt_id = create_import_batch_process_task(
        conn_meta, ibp_id, "s1_review", "Running"
    )

    records_in_count = 0
    records_failed_count = 0
    records_out_count = 0

    try:

        print("Review ingestion starting...")

        chunk_size = 1000  # Process in chunks of 1000
        chunk_count = 0

        with open(csvFilePath, encoding="utf-8") as csvf:

            cursor = conn.cursor()
            csvReader = csv.DictReader(csvf)

            while True:
                chunk = list(islice(csvReader, chunk_size))
                records_in_count += len(chunk)
                if not chunk:
                    break

                transformed_reviews = []
                failed_rows = []
                error_logs = []

                for row in chunk:

                    try:
                        # Extract and transform review data
                        reviewer_id = str(row["reviewerID"])
                        product_key = str(row["asin"])
                        reviewer_name = sanitize_string(
                            (
                                str(row["reviewerName"])
                                if row["reviewerName"]
                                else "*Unknown username"
                            )
                        )

                        if len(reviewer_id) > MAX_LENGTHS["r_reviewer_source_key"]:
                            failed_rows.append(
                                {"row": row, "error": "reviewer_id too long"}
                            )
                            continue
                        if len(product_key) > MAX_LENGTHS["r_product_key"]:
                            failed_rows.append(
                                {"row": row, "error": "product_key too long"}
                            )
                            continue
                        # if len(reviewer_name) > MAX_LENGTHS["r_reviewer_name"]:
                        #     failed_rows.append(
                        #         {"row": row, "error": "reviewer_name too long"}
                        #     )
                        #     continue

                        review_text = sanitize_string(
                            str(row["reviewText"])
                            if row["reviewText"]
                            else "*Unknown review text"
                        )

                        review_title = sanitize_string(
                            str(row["summary"]).strip()
                            if row["summary"]
                            else "*Unknown review title"
                        )
                        # if len(review_title) > MAX_LENGTHS["r_review_title"]:
                        #     failed_rows.append(
                        #         {
                        #             "row": row,
                        #             "error": "review_title too long",
                        #             "title_length": len(review_title),
                        #         }
                        #     )
                        #     continue

                        try:
                            review_score = float(row["overall"])
                        except (ValueError, TypeError):
                            failed_rows.append(
                                {"row": row, "error": "Invalid review score"}
                            )
                            continue

                        try:
                            rating_array = convert_value(row["helpful"])
                            if rating_array[1] == 0:
                                helpfullness_rating = None
                            else:
                                helpfullness_rating = float(
                                    round(rating_array[0] / rating_array[1], 2)
                                )
                        except:
                            failed_rows.append(
                                {"row": row, "error": "Invalid helpful rating"}
                            )
                            continue

                        try:
                            review_datetime = datetime.fromtimestamp(
                                int(row["unixReviewTime"])
                            )
                        except:
                            failed_rows.append(
                                {"row": row, "error": "Invalid review date time rating"}
                            )
                            continue

                        transformed_reviews.append(
                            [
                                reviewer_id,
                                product_key,
                                reviewer_name,
                                helpfullness_rating,
                                review_text,
                                review_score,
                                review_title,
                                review_datetime,
                            ]
                        )

                    except Exception as e:
                        failed_rows.append({"row": row, "error": str(e)})
                        continue

                if transformed_reviews:
                    try:
                        write_to_db(
                            cursor,
                            "s1_review",
                            [
                                "r_reviewer_source_key",
                                "r_product_key",
                                "r_reviewer_name",
                                "r_helpfulness_rating",
                                "r_review_text",
                                "r_review_score",
                                "r_review_title",
                                "r_review_datetime",
                            ],
                            transformed_reviews,
                        )
                        conn.commit()
                        records_out_count += len(transformed_reviews)
                    except Exception as e:
                        error_logs.append(
                            {
                                "chunk_error": str(e),
                                "chunk": chunk_count,
                                "entity": "reviews",
                            }
                        )

                if failed_rows:
                    write_failed_rows("./logs/review_failed_rows.json", failed_rows)
                    records_failed_count += len(failed_rows)

                if error_logs:
                    write_failed_rows("./logs/review_error_logs.json", error_logs)

                chunk_count += 1

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
        print("Review ingestion complete.")

    except KeyboardInterrupt:
        update_import_batch_process_task(
            conn_meta,
            ibpt_id,
            "Aborted",
            records_in_count,
            records_failed_count,
            records_out_count,
            None,
            None,
            None,
        )
        update_import_batch_process(conn, ibp_id, "Aborted")

    except Exception as e:
        print(f"Error during review ingestion: {e}")
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
        conn_meta.close()
        conn.close()
