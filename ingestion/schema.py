"""Validation + filtering for Wikimedia recentchange events.

The Wikimedia event envelope is large and variable; we keep only the fields the
warehouse actually uses, and drop non-edit events (logs, page moves, etc.).
"""
from __future__ import annotations

import ipaddress
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel

EDIT_TYPES = {"edit", "new"}


class EditEvent(BaseModel):
    edit_id: int
    rev_id: int | None = None
    parent_id: int | None = None
    ts: datetime
    page_title: str
    namespace: int
    wiki_db: str
    editor: str
    is_anon: bool
    is_bot: bool
    is_minor: bool
    bytes_changed: int | None = None
    comment: str | None = None

    @classmethod
    def from_event(cls, evt: dict[str, Any]) -> EditEvent | None:
        if evt.get("type") not in EDIT_TYPES:
            return None
        if evt.get("namespace") is None or evt.get("title") is None:
            return None
        try:
            ts = datetime.fromtimestamp(int(evt["timestamp"]), tz=UTC)
        except (KeyError, TypeError, ValueError, OSError):
            return None

        rev = evt.get("revision") or {}
        length = evt.get("length") or {}
        old_len = length.get("old") or 0
        new_len = length.get("new") or 0
        bytes_changed = (new_len - old_len) if length else None

        editor = (evt.get("user") or "").strip()
        if not editor:
            return None

        return cls(
            edit_id=int(rev.get("new") or evt.get("id") or 0),
            rev_id=rev.get("new"),
            parent_id=rev.get("old"),
            ts=ts,
            page_title=evt["title"],
            namespace=int(evt["namespace"]),
            wiki_db=evt.get("wiki") or "",
            editor=editor,
            is_anon=looks_like_ip(editor),
            is_bot=bool(evt.get("bot", False)),
            is_minor=bool(evt.get("minor", False)),
            bytes_changed=bytes_changed,
            comment=evt.get("comment"),
        )


def looks_like_ip(s: str) -> bool:
    try:
        ipaddress.ip_address(s)
        return True
    except ValueError:
        return False
