"""Unit tests for ingestion.schema (Wikimedia event validation)."""
from __future__ import annotations

from datetime import datetime, timezone

from schema import EditEvent, looks_like_ip


def test_looks_like_ip_v4():
    assert looks_like_ip("192.168.1.1")
    assert looks_like_ip("8.8.8.8")
    assert not looks_like_ip("not.an.ip")
    assert not looks_like_ip("Username")
    assert not looks_like_ip("")


def test_looks_like_ip_v6():
    assert looks_like_ip("2001:db8::1")
    assert looks_like_ip("::1")
    assert looks_like_ip("fe80::1")


def _base_event(**overrides):
    base = {
        "type": "edit",
        "id": 1234,
        "namespace": 0,
        "title": "Test Page",
        "comment": "fix typo",
        "timestamp": 1715000000,
        "user": "Editor",
        "bot": False,
        "minor": False,
        "wiki": "enwiki",
        "revision": {"old": 100, "new": 101},
        "length": {"old": 5000, "new": 5050},
    }
    base.update(overrides)
    return base


def test_from_event_happy_path():
    edit = EditEvent.from_event(_base_event())
    assert edit is not None
    assert edit.edit_id == 101
    assert edit.parent_id == 100
    assert edit.page_title == "Test Page"
    assert edit.wiki_db == "enwiki"
    assert edit.editor == "Editor"
    assert edit.is_anon is False
    assert edit.is_bot is False
    assert edit.bytes_changed == 50
    assert edit.ts == datetime.fromtimestamp(1715000000, tz=timezone.utc)


def test_from_event_drops_non_edit_types():
    assert EditEvent.from_event(_base_event(type="log")) is None
    assert EditEvent.from_event(_base_event(type="categorize")) is None


def test_from_event_drops_missing_required():
    assert EditEvent.from_event(_base_event(title=None)) is None
    assert EditEvent.from_event(_base_event(namespace=None)) is None
    assert EditEvent.from_event(_base_event(timestamp=None)) is None
    assert EditEvent.from_event(_base_event(user="")) is None


def test_from_event_anon_detection():
    anon_edit = EditEvent.from_event(_base_event(user="192.168.0.1"))
    assert anon_edit is not None
    assert anon_edit.is_anon is True

    user_edit = EditEvent.from_event(_base_event(user="HumanUser"))
    assert user_edit is not None
    assert user_edit.is_anon is False


def test_from_event_bot_flag():
    edit = EditEvent.from_event(_base_event(user="MyBot", bot=True))
    assert edit is not None
    assert edit.is_bot is True


def test_from_event_handles_missing_length():
    evt = _base_event()
    evt.pop("length")
    edit = EditEvent.from_event(evt)
    assert edit is not None
    assert edit.bytes_changed is None
