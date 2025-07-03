import asyncio
import logging
import os

from aiohttp import ClientTimeout

API_URL = os.getenv("OPEN_API_URL")
TIMEOUT = ClientTimeout(total=100)
MAX_CONCURRENCY = 10
SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENCY)

logger = logging.getLogger(__name__)


async def fetch_page(session, instt_code, page, semaphore):
    params = {
        "serviceKey": os.getenv("OPEN_API_KEY_PUBLIC"),
        "pageNo": page,
        "numOfRows": 1500,
        "type": "json",
        "instt_code": instt_code
    }

    async with semaphore:
        try:
            async with session.get(API_URL, params=params, timeout=TIMEOUT) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return data.get("response", {}).get("body", {}).get("items", [])
        except asyncio.TimeoutError:
            logger.error(f"TimeoutError - 기관코드: {instt_code}, 페이지: {page}")
        except Exception as e:
            logger.exception(f"기타 오류 발생 - 기관코드: {instt_code}, 페이지: {page} - {e}")
        return []


async def fetch_and_parse(session, instt_code, region_name, semaphore):
    items = []
    page = 1
    numOfRows = 1500

    while True:
        page_items = await fetch_page(session, instt_code, page, semaphore)
        if not page_items:
            break

        items.extend(page_items)

        if len(page_items) < numOfRows:
            break

        page += 1

    logger.info(f"{region_name} 수집 완료 - 총 {len(items)}건")
    return items
