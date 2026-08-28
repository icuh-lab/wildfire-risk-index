from src.crawler import fetch_all_items


def make_page_response(items, total_count):
    return {
        "response": {
            "header": {"resultCode": "00", "resultMsg": "NORMAL SERVICE."},
            "body": {"totalCount": total_count, "items": {"item": items}},
        }
    }


def item(sigucode, analdate="2026-08-28 21"):
    return {"sigucode": sigucode, "regioncode": sigucode, "analdate": analdate}


def test_모든_페이지를_끝까지_가져온다():
    total = 5
    calls = []

    def fake_fetch_page(page_no, num_of_rows):
        calls.append((page_no, num_of_rows))
        start = (page_no - 1) * num_of_rows
        page_items = [item(11110 + i) for i in range(start, min(start + num_of_rows, total))]
        return make_page_response(page_items, total)

    items = fetch_all_items(page_fetcher=fake_fetch_page, page_size=2)

    assert len(items) == total
    assert calls == [(1, 2), (2, 2), (3, 2)]


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def test_전국_조회는_localAreas를_보내지_않는다(monkeypatch):
    from src import crawler

    captured = {}

    def fake_get(url, params=None, timeout=None):
        captured["url"] = url
        captured["params"] = params
        return FakeResponse(make_page_response([item(11110)], 1))

    monkeypatch.setattr(crawler.requests, "get", fake_get)

    crawler.fetch_page(page_no=1, num_of_rows=10000)

    assert "localAreas" not in captured["params"]
    assert captured["params"]["pageNo"] == 1
    assert captured["params"]["numOfRows"] == 10000
