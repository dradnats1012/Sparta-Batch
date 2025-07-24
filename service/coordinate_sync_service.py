class CoordinateSyncService:
    def __init__(self, conn):
        self.conn = conn

    def sync(self):
        with self.conn.cursor() as cursor:
            sql = """
                INSERT INTO local_store_coordinate (
                    id, store_name, local_bill, region, address,
                    sector_name, main_product, tel_number,
                    institution_code, institution_name, created_at,
                    latitude, longitude, location
                )
                SELECT
                    id, store_name, local_bill, region, address,
                    sector_name, main_product, tel_number,
                    institution_code, institution_name, created_at,
                    latitude, longitude,
                    ST_SRID(POINT(longitude, latitude), 4326)
                FROM local_store_cleaned AS new
                WHERE new.location IS NULL
                  AND new.longitude IS NOT NULL
                  AND new.latitude IS NOT NULL
                ON DUPLICATE KEY UPDATE
                    store_name = new.store_name,
                    local_bill = new.local_bill,
                    region = new.region,
                    address = new.address,
                    sector_name = new.sector_name,
                    main_product = new.main_product,
                    tel_number = new.tel_number,
                    institution_code = new.institution_code,
                    institution_name = new.institution_name,
                    created_at = new.created_at,
                    latitude = new.latitude,
                    longitude = new.longitude,
                    location = ST_SRID(POINT(new.longitude, new.latitude), 4326);
            """
            cursor.execute(sql)
            self.conn.commit()
            print("✅ 좌표 테이블 동기화 완료")