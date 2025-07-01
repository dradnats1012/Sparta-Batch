import logging

def transform_and_upsert_cleaned_data(conn):
    with conn.cursor() as cursor:
        sql = """
              INSERT INTO local_store_cleaned (store_name, local_bill, region, address, sector_name,
                                               main_product, tel_number, institution_code, institution_name, created_at)
              SELECT affiliate_name,
                     local_bill,
                     CONCAT(ctpv_name, ' ', sgg_name) AS region,
                     CASE
                         WHEN road_addr IS NOT NULL AND road_addr != '' THEN road_addr
                         WHEN lotno_addr IS NOT NULL AND lotno_addr != '' THEN lotno_addr
                         ELSE NULL
                         END                          AS address,
                     sector_name,
                     main_prd,
                     telno,
                     instt_code,
                     instt_name,
                     crtr_ymd
              FROM local_store
              WHERE affiliate_name IS NOT NULL
                AND local_bill IS NOT NULL
                AND crtr_ymd IS NOT NULL ON DUPLICATE KEY
              UPDATE
                  sector_name =
              VALUES (sector_name), main_product =
              VALUES (main_product), tel_number =
              VALUES (tel_number), institution_name =
              VALUES (institution_name)
              """
        logging.info("정제 테이블 UPSERT 시작")
        cursor.execute(sql)
        logging.info("정제 테이블 UPSERT 완료")