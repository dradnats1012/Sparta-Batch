# build_shard_map.py
# 메인 DB에서 institution_code별 건수를 집계 → 4개 샤드 균등 배분 → shard_map 채움
import os
import re
import pymysql
from heapq import heappush, heappop, heapify
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# ===== 설정 =====
MAIN = dict(
    host=os.getenv("DB_HOST", "127.0.0.1"),
    port=int(os.getenv("DB_PORT", "3307")),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASSWORD", ""),
    db=os.getenv("DB_NAME", "sparta"),
    charset="utf8mb4",
)
SHARD_COUNT = int(os.getenv("SHARD_COUNT", "4"))   # 기본 4
SOURCE_TABLE = os.getenv("SHARD_SOURCE_TABLE", "local_store_cleaned")  # 집계 기준 테이블
SOURCE_COL   = os.getenv("SHARD_SOURCE_KEY",   "institution_code")     # 샤딩키 컬럼
# 숫자 코드만 허용할지 (테스트 코드/특수코드 제외용)
NUMERIC_ONLY = os.getenv("SHARD_NUMERIC_ONLY", "true").lower() in ("1","true","yes")

# 일부 코드를 특정 샤드에 '고정'하고 싶다면 여기에 적기 (선배치)
# 예: PINNED = {"6260000":2, "6280000":3}
PINNED = {}  # 필요시 수정

# ===== 유틸 =====
def connect_mysql(cfg: dict):
    return pymysql.connect(
        host=cfg["host"], port=int(cfg["port"]),
        user=cfg["user"], password=cfg["password"],
        db=cfg["db"], charset=cfg.get("charset","utf8mb4"),
        autocommit=True, cursorclass=pymysql.cursors.DictCursor
    )

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS shard_map (
          institution_code VARCHAR(50) PRIMARY KEY,
          shard_id TINYINT NOT NULL,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          CHECK (shard_id BETWEEN 1 AND 16)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

def fetch_counts(conn):
    # NULL/빈코드 제외
    sql = f"""
    SELECT {SOURCE_COL} AS code, COUNT(*) AS cnt
    FROM {SOURCE_TABLE}
    WHERE {SOURCE_COL} IS NOT NULL AND {SOURCE_COL} <> ''
    GROUP BY {SOURCE_COL}
    """
    rows = []
    with conn.cursor() as cur:
        cur.execute(sql)
        for r in cur.fetchall():
            code = str(r["code"]).strip()
            if NUMERIC_ONLY and not re.fullmatch(r"\d+", code):
                continue
            rows.append((code, int(r["cnt"])))
    # 큰 순서대로
    rows.sort(key=lambda x: x[1], reverse=True)
    return rows

def assign_balanced(pairs, shard_count, pinned: dict[str,int]):
    """
    pairs: List[(code, cnt)] 큰 순서로 들어오면 더 좋음
    pinned: {code: shard_id} 선배치
    return: assignments dict {code: shard_id}, totals dict {sid: sum}
    """
    # 최소힙: (sum, shard_id, list_codes)  — sum이 작은 샤드부터 배정
    heap = [(0, sid, []) for sid in range(1, shard_count+1)]
    heapify(heap)

    assignments = {}

    # 1) 고정코드 선배치
    for code, cnt in list(pairs):
        if code in pinned:
            sid = pinned[code]
            # heap에서 해당 sid 찾아 sum 갱신
            tmp = []
            target = None
            while heap:
                ssum, s, lst = heappop(heap)
                if s == sid:
                    target = (ssum, s, lst)
                    break
                tmp.append((ssum, s, lst))
            for item in tmp:
                heappush(heap, item)
            if target is None:
                # 이론상 발생X
                raise RuntimeError("Pinned shard not found in heap")

            ssum, s, lst = target
            lst.append((code, cnt))
            heappush(heap, (ssum + cnt, s, lst))
            assignments[code] = sid
            pairs.remove((code, cnt))

    # 2) 나머지 그리디 배치
    for code, cnt in pairs:
        ssum, sid, lst = heappop(heap)
        lst.append((code, cnt))
        heappush(heap, (ssum + cnt, sid, lst))
        assignments[code] = sid

    # 샤드별 합계 계산
    totals = {}
    while heap:
        ssum, sid, lst = heappop(heap)
        totals[sid] = ssum
    return assignments, totals

def build_insert_sql(assignments: dict[str,int]) -> str:
    values = ",\n".join([f"('{code}', {sid})" for code, sid in sorted(assignments.items())])
    sql = f"""INSERT INTO shard_map (institution_code, shard_id) VALUES
{values}
ON DUPLICATE KEY UPDATE shard_id=VALUES(shard_id);"""
    return sql

def main():
    print("[connect] main:", MAIN)
    conn = connect_mysql(MAIN)
    ensure_table(conn)

    pairs = fetch_counts(conn)
    total_all = sum(cnt for _, cnt in pairs)
    print(f"[info] distinct codes={len(pairs)}, total rows={total_all}")

    if PINNED:
        print(f"[info] pinned codes: {len(PINNED)} -> {PINNED}")

    assignments, totals = assign_balanced(pairs, SHARD_COUNT, PINNED)
    print("[totals] per shard:")
    for sid in sorted(totals):
        print(f"  shard{sid}: {totals[sid]} ({totals[sid]/max(1,total_all):.2%})")

    sql = build_insert_sql(assignments)

    # 실행(업서트)
    with conn.cursor() as cur:
        cur.execute(sql)
    print("[done] shard_map upserted.")

    # 확인용 몇 줄 출력
    print("\n-- sample mapping --")
    for i, (code, sid) in enumerate(sorted(assignments.items())[:20]):
        print(code, "->", sid)

if __name__ == "__main__":
    main()