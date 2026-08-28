import os
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv

from src.crawler import fetch_all_items
from src.insert_to_db import insert_items_to_db


# 원천(산림청 forestPointV2)이 현재 서비스하는 시군구는 16개 시도, 230개다.
# 행정구역 개편으로 숫자가 오르내릴 수 있어 여유를 두었다.
DEFAULT_MIN_EXPECTED_REGIONS = 200
# 시군구 총수만으로는 시도 단위 누락을 못 잡는다.
# 전남광주통합특별시(27개)가 통째로 빠져도 203개라 총수 기준을 통과해 버린다.
DEFAULT_MIN_EXPECTED_SIDO = 16


class InsufficientCoverageError(Exception):
    """원천이 기대보다 적은 시군구를 내려줬다. 행정구역 개편이나 원천 장애를 의심해야 한다."""


def region_coverage(items):
    """시도(시군구 코드 앞 2자리)별로 몇 개 시군구가 들어왔는지 센다."""
    by_sido = {}
    for code in {str(item["sigucode"]) for item in items if item.get("sigucode") is not None}:
        by_sido[code[:2]] = by_sido.get(code[:2], 0) + 1
    return by_sido


def verify_coverage(items, minimum, minimum_sido=DEFAULT_MIN_EXPECTED_SIDO):
    """
    시군구·시도 커버리지를 확인하고 부족하면 예외를 던진다.

    옛 수집기는 죽은 시군구 코드에 대해 원천이 resultCode=00 / totalCount=0을 돌려주면
    "데이터가 비어있습니다"만 찍고 성공으로 끝냈다. 그래서 전남·광주·전북이 통째로
    빠진 채 한 달 넘게 아무도 몰랐다. 부족하면 반드시 실패로 끝내야 한다.
    """
    coverage = region_coverage(items)
    total_regions = sum(coverage.values())
    total_sido = len(coverage)
    detail = dict(sorted(coverage.items()))

    print(f"커버리지: 시군구 {total_regions}개(기준 {minimum}), 시도 {total_sido}개(기준 {minimum_sido})")
    print(f"  시도별: {detail}")

    if total_regions < minimum:
        raise InsufficientCoverageError(
            f"수집된 시군구가 {total_regions}개로 기준 {minimum}개에 못 미친다. "
            f"행정구역 개편으로 원천 코드가 바뀌었거나 원천이 부분 장애일 수 있다. 시도별: {detail}"
        )

    if total_sido < minimum_sido:
        raise InsufficientCoverageError(
            f"수집된 시도가 {total_sido}개로 기준 {minimum_sido}개에 못 미친다. "
            f"시도 하나가 통째로 빠졌다. 시도별: {detail}"
        )

    return total_regions


def is_enabled(value):
    return str(value).lower() in {"1", "true", "yes", "y"}


def create_db_engine(host, port, user, password, database):
    conn_str = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
    return create_engine(conn_str, pool_pre_ping=True)


def collect_and_insert(engine, dry_run=False, minimum_regions=DEFAULT_MIN_EXPECTED_REGIONS,
                       minimum_sido=DEFAULT_MIN_EXPECTED_SIDO, items_fetcher=None):
    """전국을 한 번에 받아 커버리지를 확인한 뒤 적재한다. 커버리지가 모자라면 적재하지 않는다."""
    items = (items_fetcher or fetch_all_items)()

    verify_coverage(items, minimum_regions, minimum_sido)

    inserted = insert_items_to_db(items, engine, dry_run=dry_run)
    print(f"처리 완료: 수신={len(items)}행, 신규 적재={inserted}행, dry_run={dry_run}")
    return inserted


def main():
    load_dotenv()

    env = os.getenv("EXECUTION_ENV", "local")
    dry_run = is_enabled(os.getenv("DRY_RUN", "false"))
    minimum_regions = int(os.getenv("MIN_EXPECTED_REGIONS", DEFAULT_MIN_EXPECTED_REGIONS))
    minimum_sido = int(os.getenv("MIN_EXPECTED_SIDO", DEFAULT_MIN_EXPECTED_SIDO))
    print(f"--- 실행 환경: {env}, dry_run={dry_run}, 최소 시군구={minimum_regions}, 최소 시도={minimum_sido} ---")

    try:
        db_host = os.getenv("DB_HOST")
        db_port = int(os.getenv("DB_PORT"))
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")

        if env == "production":
            print("운영 환경으로 판단하여 RDS에 직접 접속합니다.")
            engine = create_db_engine(db_host, db_port, db_user, db_password, db_name)
            collect_and_insert(engine, dry_run=dry_run, minimum_regions=minimum_regions, minimum_sido=minimum_sido)
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
                collect_and_insert(engine, dry_run=dry_run, minimum_regions=minimum_regions, minimum_sido=minimum_sido)

    except Exception as e:
        print(f"!! 전체 프로세스 실행 중 오류가 발생했습니다: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
