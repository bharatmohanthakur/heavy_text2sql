"""Conversation store integration tests against real Postgres metadata DB."""

from __future__ import annotations

import os

import pytest

from text2sql.agent import ConversationStore
from text2sql.config import load_config


def _metadata_url() -> str:
    cfg = load_config()
    pw = (
        os.environ.get("METADATA_DB_PASSWORD")
        or os.environ.get("TARGET_DB_PASSWORD")
        or "edfi"
    )
    spec = cfg.metadata_db.model_dump()
    return (
        f"postgresql+psycopg://{spec['user']}:{pw}"
        f"@{spec['host']}:{spec['port']}/{spec['database']}"
    )


def _can_connect(url: str) -> bool:
    import sqlalchemy as sa
    try:
        sa.create_engine(url).connect().close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def store() -> ConversationStore:
    url = _metadata_url()
    if not _can_connect(url):
        pytest.skip(f"metadata DB unreachable: {url}")
    s = ConversationStore(url)
    s.drop_schema()        # clean slate
    s.ensure_schema()
    yield s
    s.drop_schema()


def test_create_and_get(store: ConversationStore) -> None:
    c = store.create_conversation(title="hello")
    assert c.id and c.title == "hello"
    fetched = store.get_conversation(c.id)
    assert fetched and fetched.id == c.id


def test_append_messages_and_replay(store: ConversationStore) -> None:
    c = store.create_conversation()
    m1 = store.append_message(c.id, role="user", content="how many students?")
    m2 = store.append_message(
        c.id, role="assistant",
        content="",
        tool_calls=[{"id": "call_1", "type": "function",
                     "function": {"name": "run_sql", "arguments": "{}"}}],
    )
    m3 = store.append_message(
        c.id, role="tool", content='{"rows": 21628}',
        tool_call_id="call_1", tool_name="run_sql",
    )
    m4 = store.append_message(c.id, role="assistant", content="21,628 students.")
    history = store.history(c.id)
    assert [m.seq for m in history] == [1, 2, 3, 4]
    assert history[0].role == "user"
    assert history[1].tool_calls[0]["function"]["name"] == "run_sql"
    assert history[2].role == "tool"
    assert history[2].tool_call_id == "call_1"
    assert history[3].content == "21,628 students."


def test_to_chat_message_shape(store: ConversationStore) -> None:
    c = store.create_conversation()
    store.append_message(c.id, role="user", content="hi")
    store.append_message(
        c.id, role="assistant", content="",
        tool_calls=[{"id": "x", "type": "function",
                     "function": {"name": "f", "arguments": "{}"}}],
    )
    store.append_message(
        c.id, role="tool", content='{"ok":true}',
        tool_call_id="x", tool_name="f",
    )
    msgs = [m.to_chat_message() for m in store.history(c.id)]
    assert msgs[0] == {"role": "user", "content": "hi"}
    assert msgs[1]["role"] == "assistant" and "tool_calls" in msgs[1]
    assert msgs[2] == {"role": "tool", "content": '{"ok":true}', "tool_call_id": "x"}


def test_list_conversations_orders_by_recent(store: ConversationStore) -> None:
    c1 = store.create_conversation(title="first")
    c2 = store.create_conversation(title="second")
    store.append_message(c1.id, role="user", content="newer activity")
    listed = store.list_conversations(limit=10)
    ids = [c.id for c in listed]
    # c1 had a more recent append, so it should rank ahead of c2
    assert ids.index(c1.id) < ids.index(c2.id)


def test_message_count_and_delete(store: ConversationStore) -> None:
    c = store.create_conversation()
    for i in range(3):
        store.append_message(c.id, role="user", content=f"msg {i}")
    assert store.message_count(c.id) == 3
    assert store.delete_conversation(c.id) is True
    assert store.get_conversation(c.id) is None
