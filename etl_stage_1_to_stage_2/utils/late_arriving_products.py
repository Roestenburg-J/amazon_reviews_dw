from psycopg2 import sql
from utils.db_utills import write_to_db


def get_missing_products(product_keys, conn):
    """Find product_keys that are missing in s2_product."""
    with conn.cursor() as cursor:
        query = sql.SQL(
            """
            SELECT DISTINCT unnest(%s) 
            EXCEPT 
            SELECT p_product_source_key FROM s2_product
            """
        )
        cursor.execute(query, [product_keys])
        return [row[0] for row in cursor.fetchall()]


def insert_placeholder_products(missing_products, conn):
    """Insert placeholder records for missing products."""
    placeholder_values = [
        (
            "*None",  # p_product_metadata_id (arbitrary placeholder ID)
            product_key,
            "*Unknow cateogry",
            -1,
            "*Unknown URL",
            "*Unknown title",
            "*Unknown description",
            -1.0,
            "*Unknown brand",
        )
        for product_key in missing_products
    ]

    with conn.cursor() as cursor:
        write_to_db(
            cursor,
            "s2_product",
            (
                "p_product_metadata_id",
                "p_product_source_key",
                "p_sales_rank_category",
                "p_sales_rank",
                "p_image_url",
                "p_title",
                "p_description",
                "p_price",
                "p_brand",
            ),
            placeholder_values,
        )
        conn.commit()
