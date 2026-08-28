import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src.insert_to_db import TABLE_NAME, insert_items_to_db


@pytest.fixture
def engine():
    return create_engine("sqlite://")


def item(sigucode, analdate="2026-08-28 21", meanavg=17):
    return {
        "analdate": analdate,
        "area": "2,399",
        "doname": "전남광주통합특별시",
        "sigun": "목포시",
        "regioncode": sigucode,
        "sigucode": sigucode,
        "meanavg": meanavg,
        "maxi": 23,
        "mini": 4,
        "std": 4,
        "upplocalcd": int(str(sigucode)[:2]),
        "d1": 100,
        "d2": 0,
        "d3": 0,
        "d4": 0,
    }


def rows_in(engine):
    with engine.connect() as conn:
        return pd.read_sql(text(f"SELECT * FROM {TABLE_NAME}"), conn)


def test_아이템_리스트를_그대로_적재한다(engine):
    inserted = insert_items_to_db([item(12110), item(12130)], engine)

    assert inserted == 2
    stored = rows_in(engine)
    assert sorted(stored["sigucode"].tolist()) == [12110, 12130]
    assert stored["area"].tolist() == [2399, 2399]


def test_이미_있는_행은_다시_넣지_않는다(engine):
    insert_items_to_db([item(12110)], engine)

    inserted = insert_items_to_db([item(12110), item(12130)], engine)

    assert inserted == 1
    assert len(rows_in(engine)) == 2


def test_dry_run이면_적재하지_않는다(engine):
    inserted = insert_items_to_db([item(12110)], engine, dry_run=True)

    assert inserted == 1
    with pytest.raises(Exception):
        rows_in(engine)
