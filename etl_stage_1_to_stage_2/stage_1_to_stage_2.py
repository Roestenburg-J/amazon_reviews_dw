import psycopg2
import time

from controllers.product import migrate_product
from controllers.review import migrate_review
from controllers.product_category import migrate_product_category
from controllers.related_product import migrate_related_product

from utils.db_utills import DB_ADW, connect_to_db
from utils.metadata_utils import (
    create_import_batch_process,
    update_import_batch_process,
    get_latest_running_import_batch,
)

if __name__ == "__main__":

    conn = connect_to_db(DB_ADW)
    ib_id = get_latest_running_import_batch(conn)

    ibp_id = create_import_batch_process(conn, ib_id, "Stage_1_to_Stage_2", "Running")

    try:

        migrate_product(ibp_id)
        migrate_review(ibp_id)
        migrate_related_product(ibp_id)
        migrate_product_category(ibp_id)

        update_import_batch_process(conn, ibp_id, "Completed")

    except Exception as e:
        update_import_batch_process(conn, ibp_id, "Failed")
    finally:
        print("Ingest Complete.")
        conn.close()
