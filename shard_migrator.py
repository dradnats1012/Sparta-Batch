#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import zlib
from typing import Dict, List, Any, Optional, Tuple

import pymysql
from pymysql.cursors import SSCursor, DictCursor
from dotenv import load_dotenv, find_dotenv

# ==========================
# .env 자동 로드
# ==========================
load_dotenv(find_dotenv())

def _env(primary: str, default: Optional[str] = None, *alts: str) -> str:
    for k in (primary, *alts):
        v = os.getenv(k)
        if v is not None and v != "":
            return v
    return default

# ==========================
# CONFIG
# ==========================
MAIN = dict(
    host=_env("DB_HOST", "127.0.0.1"),
    port=int(_env("DB_PORT", "3307")),
    user=_env("DB_USER", "root"),
    password=_env("DB_PASSWORD", ""),
    db=_env("DB_NAME", "sparta"),
    charset="utf8mb4",
)

SHARDS = {
    1: dict(
        host=_env("SHARD1_DB_HOST", _env("S1_HOST", "127.0.0.1")),
        port=int(_env("SHARD1_DB_PORT", _env("S1_PORT", "3308"))),
        user=_env("SHARD1_DB_USER", _env("S1_USER", _env("DB_USER", "root"))),
        password=_env("SHARD1_DB_PASSWORD", _env("S1_PASSWORD", _env("DB_PASSWORD", ""))),
        db=_env("SHARD1_DB_NAME", _env("S1_DB", "sparta_shard1")),
        charset="utf8mb4",
    ),
    2: dict(
        host=_env("SHARD2_DB_HOST", _env("S2_HOST", "127.0.0.1")),
        port=int(_env("SHARD2_DB_PORT", _env("S2_PORT", "3309"))),
        user=_env("SHARD2_DB_USER", _env("S2_USER", _env("DB_USER", "root"))),
        password=_env("SHARD2_DB_PASSWORD", _env("S2_PASSWORD", _env("DB_PASSWORD", ""))),
        db=_env("SHARD2_DB_NAME", _env("S2_DB", "sparta_shard2")),
        charset="utf8mb4",
    ),
    3: dict(
        host=_env("SHARD3_DB_HOST", _env("S3_HOST", "127.0.0.1")),
        port=int(_env("SHARD3_DB_PORT", _env("S3_PORT", "3310"))),
        user=_env("SHARD3_DB_USER", _env("S3_USER", _env("DB_USER", "root"))),
        password=_env("SHARD3_DB_PASSWORD", _env("S3_PASSWORD", _env("DB_PASSWORD", ""))),
        db=_env("SHARD3_DB_NAME", _env("S3_DB", "sparta_shard3")),
        charset="utf8mb4",
    ),
    4: dict(
        host=_env("SHARD4_DB_HOST", _env("S4_HOST", "127.0.0.1")),
        port=int(_env("SHARD4_DB_PORT", _env("S4_PORT", "3311"))),
        user=_env("SHARD4_DB_USER", _env("S4_USER", _env("DB_USER", "root"))),
        password=_env("SHARD4_DB_PASSWORD", _env("S4_PASSWORD", _env("DB_PASSWORD", ""))),
        db=_env("SHARD4_DB_NAME", _env("S4_DB", "sparta_shard4")),
        charset="utf8mb4",
    ),
}

BATCH_SIZE = int(_env("SHARD_BATCH_SIZE", _env("BATCH_SIZE", "10000")))
SLEEP_BETWEEN_BATCHES_SEC = float(_env("SLEEP_SEC", "0.0"))
CHECKPOINT_DIR = _env("CHECKPOINT_DIR", "./.shard_ckpt")
NULL_CODE_POLICY = _env("NULL_CODE_POLICY", "skip")  # "skip" or "force4"
RUN_TABLES = set((_env("RUN_TABLES", "cleaned,coordinate,store")).split(","))  # 선택 실행

# ==========================
# UTIL
# ==========================
def ensure_ckpt_dir():
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

def ckpt_path(table: str) -> str:
    return os.path.join(CHECKPOINT_DIR, f"{table}.ckpt")

def load_last_id(table: str) -> int:
    try:
        with open(ckpt_path(table), "r") as f:
            return int(f.read().strip())
    except Exception:
        return 0

def save_last_id(table: str, last_id: int):
    with open(ckpt_path(table), "w") as f:
        f.write(str(last_id))

def connect_mysql(cfg: dict, cursor=DictCursor):
    return pymysql.connect(
        host=cfg["host"],
        port=int(cfg["port"]),
        user=cfg["user"],
        password=cfg["password"],
        db=cfg["db"],
        charset=cfg.get("charset", "utf8mb4"),
        autocommit=False,
        cursorclass=cursor,
    )

def disable_fk(conn):
    with conn.cursor() as cur:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")

def enable_fk(conn):
    with conn.cursor() as cur:
        cur.execute("SET FOREIGN_KEY_CHECKS=1")

# ==========================
# shard_map router
# ==========================
def load_shard_map(conn) -> Dict[str, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT institution_code, shard_id FROM shard_map")
        return {r["institution_code"]: r["shard_id"] for r in cur.fetchall()}

def make_router(shard_map: Dict[str, int]):
    def router(code: Optional[str]) -> Optional[int]:
        if not code:
            return 4 if NULL_CODE_POLICY == "force4" else None
        sid = shard_map.get(code)
        if sid:
            return sid
        # (백업) 매핑에 없으면 해시로라도 분배
        return (zlib.crc32(code.encode("utf-8")) & 0xffffffff) % 4 + 1
    return router

# ==========================
# 좌표 검증/보정
# ==========================
def _valid_lon_lat(lon, lat) -> bool:
    try:
        lon = float(lon); lat = float(lat)
    except Exception:
        return False
    return -180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0

def _maybe_fix_coords(lon, lat) -> Tuple[float, float, bool]:
    if _valid_lon_lat(lon, lat):
        return float(lon), float(lat), True
    if _valid_lon_lat(lat, lon):  # 뒤집힘
        return float(lat), float(lon), True
    return lon, lat, False

# ==========================
# INSERT helpers
# ==========================
def upsert_cleaned(conn, rows: List[dict]):
    if not rows:
        return
    sql = """
    INSERT INTO local_store_cleaned
      (id, institution_code, store_name, region, address, main_product, tel_number,
       created_at, latitude, longitude, location)
    VALUES
      {values}
    ON DUPLICATE KEY UPDATE
      store_name=VALUES(store_name),
      region=VALUES(region),
      address=VALUES(address),
      main_product=VALUES(main_product),
      tel_number=VALUES(tel_number),
      created_at=VALUES(created_at),
      latitude=VALUES(latitude),
      longitude=VALUES(longitude),
      location=VALUES(location)
    """.strip()

    placeholders, params = [], []
    for r in rows:
        placeholders.append("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_SRID(POINT(%s,%s),4326))")
        params += [
            r["id"], r["institution_code"], r["store_name"], r["region"], r["address"],
            r["main_product"], r["tel_number"], r["created_at"],
            r["latitude"], r["longitude"], r["longitude"], r["latitude"]
        ]
    with conn.cursor() as cur:
        cur.execute(sql.format(values=",".join(placeholders)), params)

def upsert_coordinate(conn, rows: List[dict]):
    if not rows:
        return
    sql = """
    INSERT INTO local_store_coordinate
      (id, cleaned_id, location)
    VALUES
      {values}
    ON DUPLICATE KEY UPDATE
      cleaned_id=VALUES(cleaned_id),
      location=VALUES(location)
    """.strip()

    placeholders, params = [], []
    for r in rows:
        placeholders.append("(%s,%s,ST_SRID(POINT(%s,%s),4326))")
        params += [r["id"], r["cleaned_id"], r["lon"], r["lat"]]
    with conn.cursor() as cur:
        cur.execute(sql.format(values=",".join(placeholders)), params)

def upsert_store(conn, rows: List[dict]):
    if not rows:
        return
    cols = [
        "id","affiliate_name","local_bill","ctpv_name","sgg_name",
        "road_addr","lotno_addr","sector_name","main_prd","telno",
        "instt_code","instt_name","crtr_ymd",
    ]
    sql = f"""
    INSERT INTO local_store ({",".join(cols)})
    VALUES {{values}}
    ON DUPLICATE KEY UPDATE
      {",".join([f"{c}=VALUES({c})" for c in cols if c!="id"])}
    """.strip()

    ph = "(" + ",".join(["%s"] * len(cols)) + ")"
    values, params = [], []
    for r in rows:
        values.append(ph)
        params += [r.get(c) for c in cols]
    with conn.cursor() as cur:
        cur.execute(sql.format(values=",".join(values)), params)

# ==========================
# Migration funcs
# ==========================
def migrate_table_cleaned(main_conn, shard_conns: Dict[int, Any], router):
    table = "local_store_cleaned"
    last_id = load_last_id(table)
    print(f"[{table}] start from last_id={last_id}")
    while True:
        with main_conn.cursor() as cur:
            cur.execute("""
                SELECT id, institution_code, store_name, region, address,
                       main_product, tel_number, created_at, latitude, longitude
                FROM local_store_cleaned
                WHERE id > %s
                ORDER BY id
                LIMIT %s
            """, (last_id, BATCH_SIZE))
            rows = cur.fetchall()
        if not rows:
            print(f"[{table}] done."); break

        buckets = {1:[],2:[],3:[],4:[]}
        for r in rows:
            sid = router(r.get("institution_code"))
            if sid is None:
                continue
            buckets[sid].append(r)
            if r["id"] > last_id:
                last_id = r["id"]

        for sid, batch in buckets.items():
            if not batch:
                continue
            upsert_cleaned(shard_conns[sid], batch)
            shard_conns[sid].commit()

        save_last_id(table, last_id)
        print(f"[{table}] progressed last_id={last_id}, wrote rows={len(rows)}")
        if SLEEP_BETWEEN_BATCHES_SEC > 0:
            time.sleep(SLEEP_BETWEEN_BATCHES_SEC)

def migrate_table_coordinate(main_conn, shard_conns: Dict[int, Any], router):
    table = "local_store_coordinate"
    last_id = load_last_id(table)
    print(f"[{table}] start from last_id={last_id}")
    while True:
        with main_conn.cursor() as cur:
            cur.execute("""
                SELECT
                    c.id,
                    c.cleaned_id,
                    ST_X(c.location) AS lon,
                    ST_Y(c.location) AS lat,
                    cl.institution_code
                FROM local_store_coordinate c
                JOIN local_store_cleaned cl ON cl.id = c.cleaned_id
                WHERE c.id > %s
                ORDER BY c.id
                LIMIT %s
            """, (last_id, BATCH_SIZE))
            rows = cur.fetchall()
        if not rows:
            print(f"[{table}] done."); break

        buckets = {1:[],2:[],3:[],4:[]}
        for r in rows:
            lon, lat, ok = _maybe_fix_coords(r.get("lon"), r.get("lat"))
            if not ok:
                continue
            sid = router(r.get("institution_code"))
            if sid is None:
                continue
            r["lon"], r["lat"] = lon, lat
            buckets[sid].append(r)
            if r["id"] > last_id:
                last_id = r["id"]

        for sid, batch in buckets.items():
            if not batch:
                continue
            upsert_coordinate(shard_conns[sid], batch)
            shard_conns[sid].commit()

        save_last_id(table, last_id)
        print(f"[{table}] progressed last_id={last_id}, wrote rows={len(rows)}")
        if SLEEP_BETWEEN_BATCHES_SEC > 0:
            time.sleep(SLEEP_BETWEEN_BATCHES_SEC)

def migrate_table_store(main_conn, shard_conns: Dict[int, Any], router):
    table = "local_store"
    last_id = load_last_id(table)
    print(f"[{table}] start from last_id={last_id}")
    while True:
        with main_conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id, affiliate_name, local_bill, ctpv_name, sgg_name,
                    road_addr, lotno_addr, sector_name, main_prd, telno,
                    instt_code, instt_name, crtr_ymd
                FROM local_store
                WHERE id > %s
                ORDER BY id
                LIMIT %s
            """, (last_id, BATCH_SIZE))
            rows = cur.fetchall()
        if not rows:
            print(f"[{table}] done."); break

        buckets = {1:[],2:[],3:[],4:[]}
        for r in rows:
            sid = router(r.get("instt_code"))
            if sid is None:
                continue
            buckets[sid].append(r)
            if r["id"] > last_id:
                last_id = r["id"]

        for sid, batch in buckets.items():
            if not batch:
                continue
            upsert_store(shard_conns[sid], batch)
            shard_conns[sid].commit()

        save_last_id(table, last_id)
        print(f"[{table}] progressed last_id={last_id}, wrote rows={len(rows)}")
        if SLEEP_BETWEEN_BATCHES_SEC > 0:
            time.sleep(SLEEP_BETWEEN_BATCHES_SEC)

# ==========================
# main
# ==========================
def main():
    print("[config] MAIN:", MAIN)
    print("[config] SHARDS:", {k: {**v, "password": "***"} for k, v in SHARDS.items()})
    ensure_ckpt_dir()

    # connections
    main_conn = connect_mysql(MAIN)
    shard_conns = {sid: connect_mysql(cfg) for sid, cfg in SHARDS.items()}

    # router
    shard_map = load_shard_map(main_conn)
    router = make_router(shard_map)
    print(f"[router] shard_map entries={len(shard_map)}")

    # FK off
    for conn in shard_conns.values():
        disable_fk(conn)

    try:
        if "cleaned" in RUN_TABLES:
            migrate_table_cleaned(main_conn, shard_conns, router)
        if "coordinate" in RUN_TABLES:
            migrate_table_coordinate(main_conn, shard_conns, router)
        if "store" in RUN_TABLES:
            migrate_table_store(main_conn, shard_conns, router)
        print("[done] full distribution completed.")
    except KeyboardInterrupt:
        print("\n[warn] interrupted; checkpoints saved.")
    except Exception as e:
        print("[error]", e)
        raise
    finally:
        # FK on
        for conn in shard_conns.values():
            try:
                enable_fk(conn)
                conn.commit()
            except Exception:
                pass
        # close
        for conn in shard_conns.values():
            try:
                conn.close()
            except Exception:
                pass
        try:
            main_conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()