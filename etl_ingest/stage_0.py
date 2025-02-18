import time
from datetime import datetime

from ingest.products import ingest_products
from ingest.reviews import ingest_reviews

from utils.db_utills import DB_ADW, connect_to_db
from utils.metadata_utils import (
    create_import_batch,
    create_import_batch_process,
    update_import_batch_process,
    update_import_batch,
)


# Run the ingestion
reviews_csvFilePath = r"./data/reviews_Clothing_Shoes_and_Jewelry_5.csv"
products_csvFilePath = r"./data/metadata_category_clothing_shoes_and_jewelry_only.csv"


if __name__ == "__main__":

    conn = connect_to_db(DB_ADW)
    ib_id = create_import_batch(
        conn, "ETL run", datetime.now().year, datetime.now().month, "Running"
    )

    ibp_id = create_import_batch_process(conn, ib_id, "Ingest", "Running")

    try:
        ingest_reviews(reviews_csvFilePath, ibp_id)
        ingest_products(products_csvFilePath, ibp_id)

        update_import_batch_process(conn, ibp_id, "Completed")

    except Exception as e:
        update_import_batch_process(conn, ibp_id, "Failed")

    finally:
        print("Ingest Complete.")
        conn.close()
