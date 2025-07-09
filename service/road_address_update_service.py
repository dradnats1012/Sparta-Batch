# service/road_address_update_service.py
import time
import requests
import os
from dotenv import load_dotenv

from db.road_address_repository import fetch_target_rows, update_road_address

# 환경변수 로딩
load_dotenv()
API_KEY = os.getenv("JUSO_API_KEY")
API_URL = os.getenv("JUSO_API_URL")
MAX_ID = 200000

def convert_lotno_to_road(lotno_addr):
    params = {
        "confmKey": API_KEY,
        "currentPage": 1,
        "countPerPage": 1,
        "keyword": lotno_addr,
        "resultType": "json"
    }

    try:
        res = requests.get(API_URL, params=params, timeout=5)
        data = res.json()

        common = data.get("results", {}).get("common", {})
        error_code = common.get("errorCode", "")
        error_msg = common.get("errorMessage", "")

        if error_code != "0":
            print(f"[API 응답 오류] {lotno_addr} → 코드 {error_code}, 메시지: {error_msg}")
            return None

        juso_list = data.get("results", {}).get("juso", [])
        if not juso_list:
            print(f"[주소 없음] {lotno_addr}")
            return None

        return juso_list[0]["roadAddr"]

    except Exception as e:
        print(f"[요청 실패] {lotno_addr} → 예외: {e}")
        return None

def run_sync_batch(batch_size=100, min_id=1000000, max_id=1000000):
    batch_count = 1
    last_id = min_id - 1

    while True:
        rows = fetch_target_rows(batch_size=batch_size, min_id=last_id + 1, max_id=max_id)
        if not rows:
            print("🎉 전체 처리 완료")
            break

        for row in rows:
            lotno = row["lotno_addr"]
            road = convert_lotno_to_road(lotno)
            if road:
                print(f"[{batch_count}] 변환 성공: {lotno} → {road}")
                update_road_address(row["id"], road)
            else:
                print(f"[{batch_count}] 변환 실패: {lotno}")
            time.sleep(0.5)
            last_id = row["id"]

        batch_count += 1
        print(f"✅ {batch_count}번째 배치 완료\n")