from wiki import parse_sse_line, to_event


def make_raw(**overrides):
    raw = {
        "meta": {"id": "7f9c2b1e-0000-4000-8000-000000000001", "dt": "2026-10-28T10:00:00Z", "domain": "fr.wikipedia.org"},
        "type": "edit",
        "wiki": "frwiki",
        "title": "Apache Kafka",
        "user": "Exemple",
        "bot": False,
        "length": {"old": 1000, "new": 1250},
    }
    raw.update(overrides)
    return raw


def test_parse_sse_data_line():
    assert parse_sse_line('data: {"a": 1}') == ("data", '{"a": 1}')


def test_parse_sse_keeps_colons_in_value():
    assert parse_sse_line('id: [{"topic":"x"}]') == ("id", '[{"topic":"x"}]')


def test_parse_sse_ignores_empty_and_comments():
    assert parse_sse_line("") is None
    assert parse_sse_line(": keep-alive") is None


def test_edit_is_cleaned():
    assert to_event(make_raw()) == {
        "id": "7f9c2b1e-0000-4000-8000-000000000001",
        "ts": "2026-10-28T10:00:00Z",
        "wiki": "frwiki",
        "title": "Apache Kafka",
        "user": "Exemple",
        "bot": False,
        "type": "edit",
        "size_delta": 250,
    }


def test_new_page_has_no_old_length():
    event = to_event(make_raw(type="new", length={"new": 800}))
    assert event["size_delta"] == 800


def test_missing_length_gives_none():
    raw = make_raw()
    del raw["length"]
    assert to_event(raw)["size_delta"] is None


def test_other_types_are_ignored():
    assert to_event(make_raw(type="log")) is None
    assert to_event(make_raw(type="categorize")) is None


def test_canary_events_are_ignored():
    raw = make_raw(meta={"id": "x", "dt": "2026-10-28T10:00:00Z", "domain": "canary"})
    assert to_event(raw) is None


def test_incomplete_events_are_ignored():
    assert to_event(make_raw(meta={})) is None
    assert to_event(make_raw(title="")) is None
