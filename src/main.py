import os
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv

from src.crawler import fetch_wildfire_risk_data
from src.insert_to_db import insert_data_to_db

sigungu_codes = [
    "11000", "11110", "11140", "11170", "11200", "11215", "11230", "11230", "11260", "11290",
    "11305", "11320", "11350", "11410", "11440", "11470", "11500", "11530", "11545", "11560",
    "11590", "11620", "11650", "11680", "11710", "11740",

    "26000", "26110", "26140", "26170", "26200", "26230", "26260", "26290", "26320", "26350",
    "26380", "26410", "26440", "26470", "26500", "26530", "26710",

    "27000", "27110", "27140", "27170", "27200", "27300", "27260", "27290", "27710", "27720",

    "28000", "28110", "28140", "28177", "28185", "28200", "28237", "28245", "28260", "28271",
    "28720",

    "29000", "29110", "29140", "29155", "29170", "29200",

    "30000", "30110", "30140", "30170", "30200", "30230",

    "31000", "31110", "31140", "31170", "31200", "31710",

    "36110",

    "41000", "41110", "41130", "41150", "41170", "41190", "41210", "41220", "41250", "41270",
    "41280", "41290", "41310", "41360", "41370", "41390", "41410", "41430", "41450", "41460",
    "41480", "41500", "41550", "41570", "41590", "41610", "41630", "41650", "41670", "41800",
    "41820", "41830",

    "43000", "43110", "43130", "43150", "43720", "43730", "43740", "43745", "43750", "43760",
    "43770", "43800",

    "44000", "44130", "44150", "44180", "44200", "44210", "44230", "44250", "44270", "44710",
    "44760", "44770", "44790", "44800", "44810", "44825",

    "45000", "45110", "45130", "45140", "45180", "45190", "45210", "45710", "45720", "45730",
    "45740", "45750", "45770", "45790", "45800",

    "46000", "46110", "46130", "46150", "46170", "46230", "46710", "46720", "46730", "46770",
    "46780", "46790", "46810", "46820", "46830", "46840", "46860", "46870", "46880", "46890",
    "46900", "46910",

    "47000", "47110", "47130", "47150", "47170", "47190", "47210", "47230", "47250", "47280",
    "47290", "47730", "47750", "47760", "47770", "47820", "47830", "47840", "47850", "47900",
    "47920", "47930", "47940",

    "48000", "48120", "48170", "48220", "48240", "48250", "48270", "48310", "48330", "48720",
    "48730", "48740", "48820", "48840", "48850", "48860", "48870", "48880", "48890",

    "50000", "50110", "50130",

    "51000", "51110", "51130", "51150", "51170", "51190", "51210", "51230", "51720", "51730",
    "51750", "51760", "51770", "51780", "51790", "51800", "51810", "51820", "51830"
]


def get_sigungu_codes():
    override_codes = os.getenv("SIGUNGU_CODES")
    if override_codes:
        raw_codes = [code.strip() for code in override_codes.split(",")]
    else:
        raw_codes = sigungu_codes

    seen = set()
    filtered_codes = []
    for code in raw_codes:
        if not code or code in seen or code.endswith("000"):
            continue
        seen.add(code)
        filtered_codes.append(code)
    return filtered_codes


def is_enabled(value):
    return str(value).lower() in {"1", "true", "yes", "y"}


def create_db_engine(host, port, user, password, database):
    conn_str = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
    return create_engine(conn_str, pool_pre_ping=True)


def collect_and_insert(engine, codes, dry_run=False):
    total_inserted = 0
    total_failed = 0

    for sigungu_code in codes:
        wildfire_risk_data_json = fetch_wildfire_risk_data(sigungu_code)

        if wildfire_risk_data_json:
            total_inserted += insert_data_to_db(wildfire_risk_data_json, engine, dry_run=dry_run)
        else:
            total_failed += 1

    print(f"처리 완료: 대상={len(codes)}, 실패={total_failed}, 신규 적재={total_inserted}, dry_run={dry_run}")
    return total_inserted, total_failed


def main():
    load_dotenv()

    env = os.getenv("EXECUTION_ENV", "local")
    dry_run = is_enabled(os.getenv("DRY_RUN", "false"))
    codes = get_sigungu_codes()
    print(f"--- 실행 환경: {env} ---")
    print(f"--- 처리 대상 시군구 수: {len(codes)}, dry_run={dry_run} ---")

    try:
        db_host = os.getenv("DB_HOST")
        db_port = int(os.getenv("DB_PORT"))
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")

        if env == "production":
            print("운영 환경으로 판단하여 RDS에 직접 접속합니다.")
            engine = create_db_engine(db_host, db_port, db_user, db_password, db_name)
            collect_and_insert(engine, codes, dry_run=dry_run)
        else:
            print("로컬 환경으로 판단하여 SSH 터널링을 시작합니다.")
            from sshtunnel import SSHTunnelForwarder

            ssh_host = os.getenv("SSH_HOST")
            ssh_port = int(os.getenv("SSH_PORT"))
            ssh_user = os.getenv("SSH_USER")
            ssh_pkey = os.getenv("SSH_PKEY")

            with SSHTunnelForwarder(
                    (ssh_host, ssh_port),
                    ssh_username=ssh_user,
                    ssh_pkey=ssh_pkey,
                    remote_bind_address=(db_host, db_port)
            ) as server:
                local_port = server.local_bind_port
                print(f"SSH 터널이 생성되었습니다. (localhost:{local_port} -> {db_host}:{db_port})")

                engine = create_db_engine('127.0.0.1', local_port, db_user, db_password, db_name)
                collect_and_insert(engine, codes, dry_run=dry_run)

    except Exception as e:
        print(f"!! 전체 프로세스 실행 중 오류가 발생했습니다: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
