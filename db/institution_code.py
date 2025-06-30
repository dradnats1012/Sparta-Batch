from db.connection import get_db_connection


def get_institution_codes():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT code, region_name FROM institution_code")
            return cursor.fetchall()
    finally:
        conn.close()
