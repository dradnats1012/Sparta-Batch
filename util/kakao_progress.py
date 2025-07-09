import os
import json
from datetime import datetime

PROGRESS_FILE = "progress.json"


def load_progress() -> int:
    """
    progress.json 파일이 있으면 마지막 처리 ID를 불러오고,
    없으면 0을 반환해서 처음부터 시작하게 함.
    """
    if not os.path.exists(PROGRESS_FILE):
        return 0
    with open(PROGRESS_FILE, "r") as f:
        return json.load(f).get("last_id", 0)


def save_progress(last_id: int):
    """
    처리 완료된 마지막 ID를 저장.
    날짜도 함께 기록해 추적 가능하게 함.
    """
    with open(PROGRESS_FILE, "w") as f:
        json.dump({
            "last_id": last_id,
            "last_updated": datetime.now().strftime("%Y-%m-%d")
        }, f, indent=2)
