import pandas as pd
from sqlalchemy import text


TABLE_NAME = 'drought_impact_wildfire_risk_index'


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
    analdates = df['analdate'].dropna()

    if not sigucodes or analdates.empty:
        return df

    # analdate는 정확히 일치하는 값을 SQL에서 나열하지 않고 범위로만 좁힌다.
    # DB마다 datetime 저장 형태가 달라(예: 소수점 이하 자릿수) 문자열 동등 비교가 빗나갈 수 있는데,
    # 최종 중복 판정은 아래 existing_keys 비교가 하므로 SQL은 후보를 넉넉히 걸러오기만 하면 된다.
    start = analdates.min().strftime('%Y-%m-%d %H:%M:%S')
    end = (analdates.max() + pd.Timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')

    placeholders_codes = ', '.join(f':code_{idx}' for idx, _ in enumerate(sigucodes))
    params = {f'code_{idx}': code for idx, code in enumerate(sigucodes)}
    params.update({'start': start, 'end': end})

    query = text(f"""
        SELECT sigucode, analdate
        FROM {table_name}
        WHERE sigucode IN ({placeholders_codes})
          AND analdate >= :start
          AND analdate < :end
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


def insert_items_to_db(items, engine, dry_run=False):
    """원천에서 받은 item 리스트를 전처리한 뒤 이미 있는 행을 걸러내고 적재한다."""
    try:
        if not items:
            print("데이터가 비어있습니다.")
            return 0

        df = pd.DataFrame(items)
        df_processed = preprocess_asos_data(df)
        df_new = filter_existing_rows(df_processed, engine, TABLE_NAME)

        if df_new.empty:
            print(f"신규 데이터가 없어 '{TABLE_NAME}' 테이블 삽입을 건너뜁니다.")
            return 0

        if dry_run:
            print(f"[DRY_RUN] {len(df_new)}개의 신규 데이터가 '{TABLE_NAME}' 테이블에 삽입될 예정입니다.")
            return len(df_new)

        df_new.to_sql(TABLE_NAME, con=engine, if_exists='append', index=False)

        print(f"{len(df_new)}개의 데이터가 '{TABLE_NAME}' 테이블에 성공적으로 삽입되었습니다.")
        return len(df_new)

    except (KeyError, TypeError) as e:
        print(f"데이터 파싱 중 오류가 발생했습니다: {e}")
        return 0
    except Exception as e:
        print(f"데이터베이스 작업 중 오류가 발생했습니다: {e}")
        return 0
