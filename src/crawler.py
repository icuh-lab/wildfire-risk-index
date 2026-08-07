import os

import requests


DEFAULT_SERVICE_KEY = "YEtPMil7r0WH1I3qRagzHQ3pMd4piCG0fsUh77EI0FQjLuJ3PhgK4K6RHxyTGLd+rqqOQIfUXgEFXFr0HYgg8A=="
API_URL = "https://apis.data.go.kr/1400377/forestPointV2/forestPointListSigunguSearchV2"


def fetch_wildfire_risk_data(sigungu_code):
    service_key = os.getenv("WILDFIRE_API_SERVICE_KEY") or os.getenv("PUBLIC_DATA_SERVICE_KEY") or DEFAULT_SERVICE_KEY

    params = {
        "ServiceKey": service_key,
        "pageNo": "1",
        "numOfRows": "100",
        "_type": "json",
        "excludeForecast": os.getenv("EXCLUDE_FORECAST", "0"),
        "localAreas": sigungu_code,
    }

    try:
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "OpenAPI_ServiceResponse" in data:
            header = data["OpenAPI_ServiceResponse"].get("cmmMsgHeader", {})
            print(f"API 데이터 요청 실패({sigungu_code}): {header.get('returnAuthMsg') or header.get('errMsg')}")
            return None

        header = data.get("response", {}).get("header", {})
        if header.get("resultCode") == "00":
            body = data.get("response", {}).get("body", {})
            print(f"{sigungu_code}번 지점 데이터 수신 성공(totalCount={body.get('totalCount')})")
            return data
        else:
            print(f"API 데이터 요청 실패({sigungu_code}): {header.get('resultMsg') or 'unknown error'}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"네트워크 오류 발생({sigungu_code}): {e}")
        return None
    except ValueError as e:  # JSON 디코딩 오류 처리
        print(f"JSON 파싱 오류 발생({sigungu_code}): {e}")
        return None
