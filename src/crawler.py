import requests

def fetch_wildfire_risk_data(sigungu_code):
    url = 'http://apis.data.go.kr/1400377/forestPoint/forestPointListSigunguSearch'

    params = {
        'serviceKey': 'YEtPMil7r0WH1I3qRagzHQ3pMd4piCG0fsUh77EI0FQjLuJ3PhgK4K6RHxyTGLd+rqqOQIfUXgEFXFr0HYgg8A==',
        'pageNo': '1',
        'numOfRows': '100',
        '_type': 'json',
        'excludeForecast': '0',
        'localAreas' : sigungu_code
    }

    try:
        response = requests.get(url, params=params).json()

        if response['response']['header']['resultCode'] == '00':
            print(f"{sigungu_code}번 지점 데이터 수신 성공")
            return response
        else:
            print(f"API 데이터 요청 실패: {response['response']['header']['resultMsg']}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"네트워크 오류 발생: {e}")
        return None
    except ValueError as e:  # JSON 디코딩 오류 처리
        print(f"JSON 파싱 오류 발생: {e}")
        return None

