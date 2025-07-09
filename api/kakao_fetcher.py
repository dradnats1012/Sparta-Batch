import requests
from typing import Union, Tuple


def get_coordinates(address: str, api_key: str) -> Union[Tuple[float, float], Tuple[None, None]]:
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {
        "Authorization": f"KakaoAK {api_key}"
    }
    try:
        print(f"요청 주소: {address}")

        res = requests.get(url, headers=headers, params={"query": address}, timeout=5)

        res.raise_for_status()
        data = res.json()
        if data['documents']:
            doc = data['documents'][0]
            return float(doc['y']), float(doc['x'])
        else:
            print("좌표 반환 실패")
            return None, None
    except Exception as e:
        print(f"예외 : {e}")
        return None, None
