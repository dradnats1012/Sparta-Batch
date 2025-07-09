from db.connection import get_db_connection


def fetch_target_rows(batch_size=100, min_id=400000, max_id=500000):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
                   SELECT id, lotno_addr
                   FROM local_store
                   WHERE id >= %s
                     AND id <= %s
                     AND (road_addr IS NULL OR road_addr = '')
                     AND lotno_addr IS NOT NULL
                     AND lotno_addr != ''
                   ORDER BY id ASC
                       LIMIT %s
                   """, (min_id, max_id, batch_size))
    rows = cursor.fetchall()
    conn.close()
    return rows


def update_road_address(row_id, road_addr):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
                   UPDATE local_store
                   SET road_addr = %s
                   WHERE id = %s
                   """, (road_addr, row_id))
    conn.commit()
    conn.close()
