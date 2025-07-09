def get_batch_after_id(conn, last_id: int, batch_size: int):
    with conn.cursor() as cursor:
        cursor.execute("""
                       SELECT id, address
                       FROM local_store_cleaned
                       WHERE id > %s
                       ORDER BY id ASC
                           LIMIT %s
                       """, (last_id, batch_size))
        return cursor.fetchall()


def update_coordinates(conn, id: int, lat: float, lng: float):
    with conn.cursor() as cursor:
        cursor.execute("""
                       UPDATE local_store_cleaned
                       SET latitude  = %s,
                           longitude = %s
                       WHERE id = %s
                       """, (lat, lng, id))
