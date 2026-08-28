import os

import requests


DEFAULT_SERVICE_KEY = "YEtPMil7r0WH1I3qRagzHQ3pMd4piCG0fsUh77EI0FQjLuJ3PhgK4K6RHxyTGLd+rqqOQIfUXgEFXFr0HYgg8A=="
API_URL = "https://apis.data.go.kr/1400377/forestPointV2/forestPointListSigunguSearchV2"
DEFAULT_PAGE_SIZE = 10000


def get_service_key():
    return os.getenv("WILDFIRE_API_SERVICE_KEY") or os.getenv("PUBLIC_DATA_SERVICE_KEY") or DEFAULT_SERVICE_KEY


def fetch_page(page_no, num_of_rows):
    """
    전국 산불위험지수 한 페이지를 가져온다.

    localAreas를 보내지 않는 것이 핵심이다. 시군구 코드를 지정하면 원천이 아는 코드만
    받아올 수 있는데, 행정구역이 개편되면 우리가 들고 있던 코드가 조용히 죽는다
    (구 전남 46xxx·광주 29xxx·전북 45xxx가 실제로 그렇게 통째로 누락됐다).
    지정하지 않으면 원천이 현재 서비스하는 시군구 전체를 그대로 내려준다.
    """
    params = {
        "ServiceKey": get_service_key(),
        "pageNo": page_no,
        "numOfRows": num_of_rows,
        "_type": "json",
        "excludeForecast": os.getenv("EXCLUDE_FORECAST", "0"),
    }

    try:
        response = requests.get(API_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        if "OpenAPI_ServiceResponse" in data:
            header = data["OpenAPI_ServiceResponse"].get("cmmMsgHeader", {})
            print(f"API 데이터 요청 실패(page={page_no}): {header.get('returnAuthMsg') or header.get('errMsg')}")
            return None

        header = data.get("response", {}).get("header", {})
        if header.get("resultCode") != "00":
            print(f"API 데이터 요청 실패(page={page_no}): {header.get('resultMsg') or 'unknown error'}")
            return None

        return data
    except requests.exceptions.RequestException as e:
        print(f"네트워크 오류 발생(page={page_no}): {e}")
        return None
    except ValueError as e:  # JSON 디코딩 오류 처리
        print(f"JSON 파싱 오류 발생(page={page_no}): {e}")
        return None


def extract_page_items(data):
    """페이지 응답에서 item 리스트를 꺼낸다. 응답 형태가 dict/list로 흔들려도 리스트로 통일한다."""
    items = data.get("response", {}).get("body", {}).get("items")

    if not items:
        return []

    item = items.get("item", []) if isinstance(items, dict) else items

    if isinstance(item, dict):
        return [item]
    if isinstance(item, list):
        return item
    return []


def fetch_all_items(page_fetcher=None, page_size=DEFAULT_PAGE_SIZE):
    """localAreas 없이 전국을 페이지 단위로 끝까지 가져온다."""
    fetch = page_fetcher or fetch_page
    collected = []
    page_no = 1

    while True:
        data = fetch(page_no, page_size)
        if data is None:
            break

        items = extract_page_items(data)
        collected.extend(items)

        total_count = int(data.get("response", {}).get("body", {}).get("totalCount") or 0)
        if len(collected) >= total_count or not items:
            break

        page_no += 1

    print(f"원천 수신 완료: {len(collected)}행")
    return collected
