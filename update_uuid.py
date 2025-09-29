#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import uuid
from typing import Dict, Any, List, Tuple

import pymysql
from pymysql.cursors import DictCursor
from dotenv import load_dotenv, find_dotenv

# -------------------------
# .env 로드
# -------------------------
load_dotenv(find_dotenv())

def _env(primary: str, default: str = "", *alts: str) -> str:
    for k in (primary, *alts):
        v = os.getenv(k)
        if v:
            return v
    return default

MAIN = dict(
    host=_env("DB_HOST", "127.0.0.1"),
    port=int(_env("DB_PORT", "3307")),
    user=_env("DB_USER", "root"),
    password=_env("DB_PASSWORD", ""),
    db=_env("DB_NAME", "sparta"),
    charset="utf8mb4",
)

def connect_mysql(cfg: Dict[str, Any]):
    return pymysql.connect(
        host=cfg["host"],
        port=int(cfg["port"]),
        user=cfg["user"],
        password=cfg["password"],
        db=cfg["db"],
        charset=cfg.get("charset", "utf8mb4"),
        autocommit=False,
        cursorclass=DictCursor,
    )

# -------------------------
# UUID v4 채우기 (BINARY(16) 가정)
# -------------------------
def update_uuid_v4_one_conn(conn, table_name: str) -> Tuple[int, int]:
    """해당 커넥션의 table에서 uuid IS NULL인 row에 uuidv4(bytes) 채움.
    return: (총 NULL 건수, 업데이트 성공 건수)
    """
    null_count = 0
    updated = 0
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT id FROM {table_name} WHERE uuid IS NULL")
        rows: List[Dict[str, Any]] = cursor.fetchall()
        null_count = len(rows)
        if null_count == 0:
            return (0, 0)

        update_sql = f"UPDATE {table_name} SET uuid = %s WHERE id = %s"
        for r in rows:
            new_uuid = uuid.uuid4().bytes  # BINARY(16) 컬럼에 적합
            cursor.execute(update_sql, (new_uuid, r["id"]))
            updated += 1
    conn.commit()
    return (null_count, updated)

def update_uuid_v4_all(table_name: str):
    # 메인 DB
    print(f"[main:{MAIN['db']}] {table_name} 처리 시작")
    main_conn = connect_mysql(MAIN)
    try:
        n, u = update_uuid_v4_one_conn(main_conn, table_name)
        print(f"[main:{MAIN['db']}] NULL={n}, UPDATED={u}")
    finally:
        main_conn.close()


if __name__ == "__main__":
    update_uuid_v4_all("local_store_cleaned")
    update_uuid_v4_all("institution_code")