import os
import aiohttp
from typing import Optional
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("JUSO_API_KEY")
API_URL = os.getenv("JUSO_API_URL")

async def fetch_road_address(session, lotno_addr: str) -> Optional[str]:
    params = {
        "confmKey": API_KEY,
        "currentPage": 1,
        "countPerPage": 1,
        "keyword": lotno_addr,
        "resultType": "json"
    }

    try:
        async with session.get(API_URL, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            data = await resp.json()
            if data["results"]["common"]["errorCode"] == "0" and data["results"]["juso"]:
                return data["results"]["juso"][0]["roadAddr"]
    except Exception as e:
        print(f"실패 {lotno_addr} → {e}")
    return None