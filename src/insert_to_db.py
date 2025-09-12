import pandas as pd

def preprocess_asos_data(df):
    df = df.replace('', None)

    # area 컬럼의 쉼표(,)를 제거합니다.
    # .str accessor를 사용하기 위해 먼저 문자열 타입으로 변환해주는 것이 안전합니다.
    if 'area' in df.columns:
        df['area'] = df['area'].astype(str).str.replace(',', '')

    # 숫자형으로 변환할 컬럼 목록
    numeric_cols = [
        'area', 'd1', 'd2', 'd3', 'd4', 'maxi', 'meanavg',
        'mini', 'regioncode', 'sigucode', 'std', 'upplocalcd'
    ]

    for col in numeric_cols:
        # errors='coerce'는 변환할 수 없는 값을 NaT(Not a Time) 또는 NaN(Not a Number)으로 처리
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 날짜 컬럼을 datetime 형식으로 변환
    df['analdate'] = pd.to_datetime(df['analdate'], format='%Y-%m-%d %H', errors='coerce')

    return df


def insert_data_to_db(json_response, engine):
    """
        JSON 응답을 받아 파싱, 전처리 후 데이터베이스에 삽입합니다.
        """
    try:
        # 1. JSON에서 실제 데이터 리스트 추출
        items = json_response['response']['body']['items']['item']
        if not items:
            print("데이터가 비어있습니다.")
            return
        # 매핑된 테이블 이름 가져오기
        table_name = 'drought_impact_wildfire_risk_index'

        # 2. 리스트를 Pandas DataFrame으로 변환
        df = pd.DataFrame(items)

        # 3. 데이터 전처리
        df_processed = preprocess_asos_data(df)

        # 4. DataFrame을 SQL 테이블에 삽입
        # table_name: 실제 DB에 생성할 테이블 이름
        # if_exists='append': 테이블이 존재하면 데이터 추가 (다른 옵션: 'replace', 'fail')
        # index=False: DataFrame의 index는 DB에 추가하지 않음
        df_processed.to_sql(table_name, con=engine, if_exists='append', index=False)

        print(f"{len(df_processed)}개의 데이터가 '{table_name}' 테이블에 성공적으로 삽입되었습니다.")

    except (KeyError, TypeError) as e:
        print(f"JSON 데이터 파싱 중 오류가 발생했습니다: {e}")
    except Exception as e:
        print(f"데이터베이스 작업 중 오류가 발생했습니다: {e}")