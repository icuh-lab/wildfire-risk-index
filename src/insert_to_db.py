import pandas as pd
from sqlalchemy import text


TABLE_NAME = 'drought_impact_wildfire_risk_index'


def extract_items(json_response):
    items = json_response.get('response', {}).get('body', {}).get('items')

    if not items:
        return []

    if isinstance(items, dict):
        item = items.get('item', [])
    else:
        item = items

    if isinstance(item, dict):
        return [item]
    if isinstance(item, list):
        return item
    return []


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
        if col in df.columns:
            # errors='coerce'는 변환할 수 없는 값을 NaT(Not a Time) 또는 NaN(Not a Number)으로 처리
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 날짜 컬럼을 datetime 형식으로 변환
    if 'analdate' in df.columns:
        df['analdate'] = pd.to_datetime(df['analdate'], format='%Y-%m-%d %H', errors='coerce')

    return df


def filter_existing_rows(df, engine, table_name):
    if df.empty or not {'sigucode', 'analdate'}.issubset(df.columns):
        return df

    sigucodes = [int(code) for code in df['sigucode'].dropna().unique().tolist()]
    analdates = df['analdate'].dropna().dt.strftime('%Y-%m-%d %H:%M:%S').unique().tolist()

    if not sigucodes or not analdates:
        return df

    placeholders_codes = ', '.join(f':code_{idx}' for idx, _ in enumerate(sigucodes))
    placeholders_dates = ', '.join(f':date_{idx}' for idx, _ in enumerate(analdates))
    params = {f'code_{idx}': code for idx, code in enumerate(sigucodes)}
    params.update({f'date_{idx}': analdate for idx, analdate in enumerate(analdates)})

    query = text(f"""
        SELECT sigucode, analdate
        FROM {table_name}
        WHERE sigucode IN ({placeholders_codes})
          AND analdate IN ({placeholders_dates})
    """)

    try:
        existing = pd.read_sql(query, con=engine, params=params)
    except Exception:
        return df

    if existing.empty:
        return df

    existing['analdate'] = pd.to_datetime(existing['analdate'], errors='coerce')
    existing_keys = set(zip(existing['sigucode'].astype('Int64'), existing['analdate']))
    keep_mask = ~df.apply(lambda row: (row['sigucode'], row['analdate']) in existing_keys, axis=1)
    return df[keep_mask]


def insert_data_to_db(json_response, engine, dry_run=False):
    """
        JSON 응답을 받아 파싱, 전처리 후 데이터베이스에 삽입합니다.
        """
    try:
        # 1. JSON에서 실제 데이터 리스트 추출
        items = extract_items(json_response)
        if not items:
            print("데이터가 비어있습니다.")
            return 0

        # 2. 리스트를 Pandas DataFrame으로 변환
        df = pd.DataFrame(items)

        # 3. 데이터 전처리
        df_processed = preprocess_asos_data(df)
        df_new = filter_existing_rows(df_processed, engine, TABLE_NAME)

        if df_new.empty:
            print(f"신규 데이터가 없어 '{TABLE_NAME}' 테이블 삽입을 건너뜁니다.")
            return 0

        if dry_run:
            print(f"[DRY_RUN] {len(df_new)}개의 신규 데이터가 '{TABLE_NAME}' 테이블에 삽입될 예정입니다.")
            return len(df_new)

        # 4. DataFrame을 SQL 테이블에 삽입
        # table_name: 실제 DB에 생성할 테이블 이름
        # if_exists='append': 테이블이 존재하면 데이터 추가 (다른 옵션: 'replace', 'fail')
        # index=False: DataFrame의 index는 DB에 추가하지 않음
        df_new.to_sql(TABLE_NAME, con=engine, if_exists='append', index=False)

        print(f"{len(df_new)}개의 데이터가 '{TABLE_NAME}' 테이블에 성공적으로 삽입되었습니다.")
        return len(df_new)

    except (KeyError, TypeError) as e:
        print(f"JSON 데이터 파싱 중 오류가 발생했습니다: {e}")
        return 0
    except Exception as e:
        print(f"데이터베이스 작업 중 오류가 발생했습니다: {e}")
        return 0
