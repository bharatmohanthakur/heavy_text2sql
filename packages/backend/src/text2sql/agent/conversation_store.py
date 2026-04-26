"""Postgres-backed conversation history.

Two tables in `text2sql_meta`:

  conversation
    id           UUID  PK
    title        text  (auto-summary or user-set)
    created_at   timestamptz
    last_active  timestamptz

  conversation_message
    id              UUID PK
    conversation_id UUID FK
    seq             int    (monotonic per conversation)
    role            text   (user | assistant | tool)
    content         text   (assistant/user free text; tool: JSON result)
    tool_calls      JSONB  (assistant turns that called tools; null otherwise)
    tool_call_id    text   (only on tool messages, ties result to a call)
    tool_name       text   (only on tool messages)
    created_at      timestamptz

The store is the single source of truth for a conversation; every agent turn
reads history from here, calls the LLM, persists assistant + tool messages.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class _Base(DeclarativeBase):
    pass


class ConversationRow(_Base):
    __tablename__ = "conversation"

    id = sa.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    title = sa.Column(Text, nullable=False, default="")
    created_at = sa.Column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    last_active = sa.Column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class ConversationMessageRow(_Base):
    __tablename__ = "conversation_message"

    id = sa.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    conversation_id = sa.Column(
        UUID(as_uuid=True),
        ForeignKey("conversation.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    seq = sa.Column(Integer, nullable=False)
    role = sa.Column(String(16), nullable=False)
    content = sa.Column(Text, nullable=False, default="")
    tool_calls = sa.Column(JSONB, nullable=True)
    tool_call_id = sa.Column(String(64), nullable=True)
    tool_name = sa.Column(String(64), nullable=True)
    created_at = sa.Column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        sa.UniqueConstraint("conversation_id", "seq", name="uq_conv_seq"),
    )


# ── Public dataclasses ───────────────────────────────────────────────────────


@dataclass
class Conversation:
    id: uuid.UUID
    title: str
    created_at: datetime
    last_active: datetime

    @classmethod
    def from_row(cls, r: ConversationRow) -> "Conversation":
        return cls(
            id=r.id, title=r.title or "",
            created_at=r.created_at, last_active=r.last_active,
        )


@dataclass
class ConversationMessage:
    id: uuid.UUID
    conversation_id: uuid.UUID
    seq: int
    role: str                              # "user" | "assistant" | "tool"
    content: str                           # text body or JSON-encoded tool result
    tool_calls: list[dict[str, Any]] | None
    tool_call_id: str | None
    tool_name: str | None
    created_at: datetime

    @classmethod
    def from_row(cls, r: ConversationMessageRow) -> "ConversationMessage":
        return cls(
            id=r.id, conversation_id=r.conversation_id, seq=r.seq,
            role=r.role, content=r.content or "",
            tool_calls=list(r.tool_calls) if r.tool_calls else None,
            tool_call_id=r.tool_call_id, tool_name=r.tool_name,
            created_at=r.created_at,
        )

    def to_chat_message(self) -> dict[str, Any]:
        """Translate to the OpenAI chat-completions message shape."""
        if self.role == "tool":
            return {
                "role": "tool",
                "content": self.content,
                "tool_call_id": self.tool_call_id or "",
            }
        msg: dict[str, Any] = {"role": self.role, "content": self.content}
        if self.tool_calls:
            msg["tool_calls"] = self.tool_calls
        return msg


# ── Store ────────────────────────────────────────────────────────────────────


class ConversationStore:
    def __init__(self, sa_url: str) -> None:
        self._engine = sa.create_engine(sa_url, future=True, pool_pre_ping=True)
        self._Session: sessionmaker[Session] = sessionmaker(
            bind=self._engine, expire_on_commit=False,
        )

    def ensure_schema(self) -> None:
        """Create only the conversation tables. Idempotent."""
        _Base.metadata.create_all(self._engine)

    def drop_schema(self) -> None:
        """Drop ONLY the conversation tables (`conversation`,
        `conversation_message`).

        SQLAlchemy `_Base.metadata.drop_all` is already scoped to tables
        registered against this module's `_Base`, but we keep the call
        local-only by spelling out which tables are owned here. Other
        artifacts in the metadata DB (e.g. `gold_sql` from
        text2sql.gold.GoldStore) belong to different DeclarativeBases
        and are unaffected.
        """
        _Base.metadata.drop_all(
            self._engine,
            tables=[ConversationMessageRow.__table__, ConversationRow.__table__],
        )

    # ── Conversation CRUD ────────────────────────────────────────────────────

    def create_conversation(self, title: str = "") -> Conversation:
        with self._Session.begin() as s:
            row = ConversationRow(title=title)
            s.add(row)
            s.flush()
            return Conversation.from_row(row)

    def get_conversation(self, conv_id: uuid.UUID) -> Conversation | None:
        with self._Session() as s:
            row = s.get(ConversationRow, conv_id)
            return Conversation.from_row(row) if row else None

    def list_conversations(self, *, limit: int = 50) -> list[Conversation]:
        stmt = (
            sa.select(ConversationRow)
            .order_by(ConversationRow.last_active.desc())
            .limit(limit)
        )
        with self._Session() as s:
            return [Conversation.from_row(r) for r in s.scalars(stmt)]

    def set_title(self, conv_id: uuid.UUID, title: str) -> None:
        with self._Session.begin() as s:
            row = s.get(ConversationRow, conv_id)
            if row is not None:
                row.title = title

    def delete_conversation(self, conv_id: uuid.UUID) -> bool:
        with self._Session.begin() as s:
            row = s.get(ConversationRow, conv_id)
            if row is None:
                return False
            s.delete(row)
            return True

    # ── Message CRUD ─────────────────────────────────────────────────────────

    def append_message(
        self,
        conv_id: uuid.UUID,
        *,
        role: str,
        content: str = "",
        tool_calls: list[dict[str, Any]] | None = None,
        tool_call_id: str | None = None,
        tool_name: str | None = None,
    ) -> ConversationMessage:
        with self._Session.begin() as s:
            seq = s.scalar(
                sa.select(sa.func.coalesce(sa.func.max(ConversationMessageRow.seq), 0))
                .where(ConversationMessageRow.conversation_id == conv_id)
            ) or 0
            row = ConversationMessageRow(
                conversation_id=conv_id,
                seq=seq + 1,
                role=role,
                content=content,
                tool_calls=tool_calls,
                tool_call_id=tool_call_id,
                tool_name=tool_name,
            )
            s.add(row)
            # Bump conversation's last_active
            conv = s.get(ConversationRow, conv_id)
            if conv is not None:
                conv.last_active = datetime.now(timezone.utc)
            s.flush()
            return ConversationMessage.from_row(row)

    def history(self, conv_id: uuid.UUID) -> list[ConversationMessage]:
        stmt = (
            sa.select(ConversationMessageRow)
            .where(ConversationMessageRow.conversation_id == conv_id)
            .order_by(ConversationMessageRow.seq)
        )
        with self._Session() as s:
            return [ConversationMessage.from_row(r) for r in s.scalars(stmt)]

    def message_count(self, conv_id: uuid.UUID) -> int:
        stmt = (
            sa.select(sa.func.count(ConversationMessageRow.id))
            .where(ConversationMessageRow.conversation_id == conv_id)
        )
        with self._Session() as s:
            return int(s.scalar(stmt) or 0)
