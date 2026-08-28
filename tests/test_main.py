import pytest

from src.main import InsufficientCoverageError, region_coverage, verify_coverage

# 원천이 현재 서비스하는 16개 시도. 12=전남광주통합특별시, 52=전북특별자치도.
SIDO_PREFIXES = ["11", "12", "26", "27", "28", "30", "31", "36",
                 "41", "43", "44", "47", "48", "50", "51", "52"]


def spread(total, prefixes=SIDO_PREFIXES):
    """시군구 코드 total개를 주어진 시도들에 고르게 흩뿌린다."""
    codes = []
    for i in range(total):
        prefix = prefixes[i % len(prefixes)]
        codes.append(int(f"{prefix}{110 + (i // len(prefixes)) * 10:03d}"))
    return codes


def items_for(sigucodes):
    """시군구 코드마다 예보 3행씩 있는 원천 응답을 흉내낸다."""
    return [
        {"sigucode": code, "regioncode": code, "upplocalcd": int(str(code)[:2]), "analdate": f"2026-08-28 {hour:02d}"}
        for code in sigucodes
        for hour in (15, 18, 21)
    ]


def test_전남_전북_광주가_빠진_수집결과는_실패로_처리한다():
    """2026-08 실제 사고 재현: 원천 230개 중 181개만 들어와도 예전엔 성공으로 끝났다."""
    collected = items_for(spread(181))

    with pytest.raises(InsufficientCoverageError, match="시군구가"):
        verify_coverage(collected, minimum=200, minimum_sido=16)


def test_시도가_통째로_빠지면_시군구_총수가_기준을_넘어도_실패한다():
    """
    전남광주통합특별시(27개)만 빠지면 230 - 27 = 203개라 총수 기준(200)은 통과한다.
    시도 단위 누락은 총수로 못 잡으므로 시도 개수로 따로 막아야 한다.
    """
    전남광주_빠진_시도 = [p for p in SIDO_PREFIXES if p != "12"]
    collected = items_for(spread(203, 전남광주_빠진_시도))

    with pytest.raises(InsufficientCoverageError, match="시도가"):
        verify_coverage(collected, minimum=200, minimum_sido=16)


def test_전국이_다_들어오면_시군구_수를_돌려준다():
    collected = items_for(spread(230))

    assert verify_coverage(collected, minimum=200, minimum_sido=16) == 230


def test_시도별_시군구_수를_센다():
    collected = items_for([11110, 11140, 12110, 52110])

    assert region_coverage(collected) == {"11": 2, "12": 1, "52": 1}


def test_커버리지가_충족되면_적재하고_건수를_돌려준다():
    from sqlalchemy import create_engine
    from src.main import collect_and_insert

    engine = create_engine("sqlite://")
    collected = items_for(spread(230))

    inserted = collect_and_insert(engine, minimum_regions=200, items_fetcher=lambda: collected)

    assert inserted == len(collected)


def test_커버리지가_미달이면_적재하지_않고_예외를_던진다():
    from sqlalchemy import create_engine, inspect
    from src.main import collect_and_insert

    engine = create_engine("sqlite://")
    collected = items_for(spread(181))

    with pytest.raises(InsufficientCoverageError):
        collect_and_insert(engine, minimum_regions=200, items_fetcher=lambda: collected)

    assert inspect(engine).get_table_names() == []
