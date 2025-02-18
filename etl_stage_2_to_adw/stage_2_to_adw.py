import psycopg2
import time


from utils.db_utills import DB_ADW, connect_to_db
from utils.metadata_utils import (
    create_import_batch_process,
    update_import_batch_process,
    get_latest_running_import_batch,
    update_import_batch,
)

from controllers.product import migrate_product_to_adw
from controllers.category import migrate_category_to_adw
from controllers.product_category_bridge import migrate_product_category_bridge_to_adw
from controllers.related_product import migrate_related_product_to_adw
from controllers.review_descriptors import migrate_review_descriptors_to_adw
from controllers.reviewer import migrate_reviewer_to_adw
from controllers.review_fact import migrate_fact_table_to_adw

if __name__ == "__main__":
    print("starting")

    try:

        conn = connect_to_db(DB_ADW)
        ib_id = get_latest_running_import_batch(conn)
        # print(f"here is the ib_id: {ib_id}")

        ibp_id = create_import_batch_process(conn, ib_id, "Stage_2_to_ADW", "Running")

        # migrate_product_to_adw(ibp_id)
        # migrate_category_to_adw(ibp_id)
        # migrate_product_category_bridge_to_adw(ibp_id)
        # migrate_related_product_to_adw(ibp_id)
        # migrate_review_descriptors_to_adw(ib_id)
        # migrate_reviewer_to_adw(ibp_id)
        migrate_fact_table_to_adw(ibp_id)

        update_import_batch_process(conn, ibp_id, "Completed")
        # update_import_batch(conn, ib_id, "Completed")

    except Exception as e:
        print(f"error: {e}")
        update_import_batch_process(conn, ibp_id, "Failed")
        # update_import_batch(conn, ib_id, "Completed")
    finally:
        print("Ingest Complete.")
        conn.close()
