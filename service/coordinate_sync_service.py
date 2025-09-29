class CoordinateSyncService:
    def __init__(self, conn):
        self.conn = conn

    def sync(self):
        with self.conn.cursor() as cursor:
            update_sql = """
                UPDATE local_store_coordinate AS coord
                JOIN local_store_cleaned AS new
                    ON new.id = coord.cleaned_id
                SET coord.location = ST_SRID(POINT(new.longitude, new.latitude), 4326)
                WHERE new.longitude IS NOT NULL
                  AND new.latitude IS NOT NULL;
            """
            cursor.execute(update_sql)

            insert_sql = """
                INSERT INTO local_store_coordinate (cleaned_id, location)
                SELECT
                    new.id,
                    ST_SRID(POINT(new.longitude, new.latitude), 4326)
                FROM local_store_cleaned AS new
                LEFT JOIN local_store_coordinate AS coord
                    ON coord.cleaned_id = new.id
                WHERE new.longitude IS NOT NULL
                  AND new.latitude IS NOT NULL
                  AND coord.cleaned_id IS NULL;
            """
            cursor.execute(insert_sql)

        self.conn.commit()
        print("✅ 좌표 테이블 동기화 완료")
