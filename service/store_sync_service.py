import asyncio
import logging
from aiohttp import ClientSession
from api.store_fetcher import fetch_and_parse
from db.institution_code import get_institution_codes
from db.raw_store_repository import upsert_store_data

async def sync_one_region(session, code, region_name, semaphore, conn):
    try:
        items = await fetch_and_parse(session, code, region_name, semaphore)
        if not items:
            logging.warning(f"{region_name} 데이터 없음")
        else:
            upsert_store_data(conn, items)
    except Exception as e:
        logging.exception(f"{region_name} 처리 중 오류: {e}")

async def sync_all_regions(conn):
    codes = get_institution_codes()
    semaphore = asyncio.Semaphore(10)

    async with ClientSession() as session:
        tasks = [
            sync_one_region(session, code["code"], code["region_name"], semaphore, conn)
            for code in codes
        ]
        await asyncio.gather(*tasks, return_exceptions=True)