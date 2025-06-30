import asyncio

from config.logging import setup_logging
from db.connection import get_db_connection
from service.sync_service import sync_all_regions
from service.transform_service import transform_and_upsert_cleaned_data

if __name__ == "__main__":
    setup_logging()
    conn = get_db_connection()
    asyncio.run(sync_all_regions(conn))
    transform_and_upsert_cleaned_data(conn)
    conn.close()
