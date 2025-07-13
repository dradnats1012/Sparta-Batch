import logging


def convert_keys(item):
    return {
        "affiliate_name": item.get("affiliateNm"),
        "local_bill": item.get("localBill"),
        "ctpv_name": item.get("ctpvNm"),
        "sgg_name": item.get("sggNm"),
        "road_addr": item.get("lctnRoadNmAddr"),
        "lotno_addr": item.get("lctnLotnoAddr"),
        "sector_name": item.get("sectorNm"),
        "main_prd": item.get("mainPrd"),
        "telno": item.get("telno"),
        "instt_code": item.get("insttCode"),
        "instt_name": item.get("insttNm"),
        "crtr_ymd": item.get("crtrYmd")
    }


def upsert_store_data(conn, items):
    if not items:
        return

    with conn.cursor() as cursor:
        sql = """
              INSERT INTO local_store (affiliate_name, local_bill, ctpv_name, sgg_name,
                                       road_addr, lotno_addr, sector_name, main_prd,
                                       telno, instt_code, instt_name, crtr_ymd)
              VALUES (%(affiliate_name)s, %(local_bill)s, %(ctpv_name)s, %(sgg_name)s,
                      %(road_addr)s, %(lotno_addr)s, %(sector_name)s, %(main_prd)s,
                      %(telno)s, %(instt_code)s, %(instt_name)s, %(crtr_ymd)s) ON DUPLICATE KEY
              UPDATE
                  sector_name =
              VALUES (sector_name), main_prd =
              VALUES (main_prd), telno =
              VALUES (telno), instt_name =
              VALUES (instt_name)
              """

        for item in items:
            try:
                mapped = convert_keys(item)

                if not mapped["road_addr"]:
                    mapped["road_addr"] = mapped["lotno_addr"]

                cursor.execute(sql, mapped)
            except Exception as e:
                logging.error(f"❌ UPSERT 실패 - {item.get('affiliateNm', '알 수 없음')}: {e}")
