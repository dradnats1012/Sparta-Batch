import logging
from db.cleaned_store_repository import transform_and_upsert_cleaned_data
from db.connection import get_db_connection


def run_transform_cleaned_store():
    conn = get_db_connection()
    try:
        transform_and_upsert_cleaned_data(conn)
    except Exception as e:
        logging.exception("정제 테이블 변환 중 오류 발생")
    finally:
        conn.close()
