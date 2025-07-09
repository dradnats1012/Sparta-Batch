import os
import time
import logging
from dotenv import load_dotenv
from api.kakao_fetcher import get_coordinates
from db.connection import get_db_connection
from db.kakao_cleaned_store_repository import get_batch_after_id, update_coordinates
from util.kakao_progress import load_progress, save_progress
from config.kakao_logging import setup_logging


class KakaoCoordinateUpdateService:
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv("KAKAO_API_KEY")
        self.batch_size = 95000
        self.conn = get_db_connection()
        setup_logging()

    def run(self):
        last_id = load_progress()
        rows = get_batch_after_id(self.conn, last_id, self.batch_size)

        if not rows:
            print("완료: 더 이상 처리할 데이터가 없습니다.")
            return

        for row in rows:
            id = row["id"]
            address = row["address"]

            lat, lng = get_coordinates(address, self.api_key)
            if lat is None or lng is None:
                logging.getLogger("fail").info(f"id={id}, address='{address}', reason=coordinate_fetch_failed")
                continue

            try:
                update_coordinates(self.conn, id, lat, lng)
            except Exception as e:
                logging.getLogger("fail").info(f"id={id}, address='{address}', reason=update_failed: {e}")
                continue

            time.sleep(0.05)

        self.conn.commit()
        save_progress(rows[-1]["id"])
        print(f"{len(rows)}개 처리완료. 마지막 ID: {rows[-1]['id']}")