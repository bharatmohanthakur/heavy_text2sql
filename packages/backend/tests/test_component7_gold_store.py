"""Component 7 integration tests against real Postgres + real Azure embeddings.

Each test runs against a fresh schema in `text2sql_meta`. We tear down at
the end so re-runs are clean.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

from text2sql.config import REPO_ROOT, load_config
from text2sql.gold import GoldStore
from text2sql.gold.ast_flatten import flatten_sql_ast


def _has_azure() -> bool:
    return bool(os.environ.get("AZURE_OPENAI_API_KEY"))


def _metadata_url() -> str | None:
    cfg = load_config()
    pw = os.environ.get("METADATA_DB_PASSWORD") or os.environ.get("TARGET_DB_PASSWORD") or "edfi"
    spec = cfg.metadata_db.model_dump()
    return (
        f"postgresql+psycopg://{spec['user']}:{pw}"
        f"@{spec['host']}:{spec['port']}/{spec['database']}"
    )


def _can_connect(url: str) -> bool:
    import sqlalchemy as sa
    try:
        sa.create_engine(url, future=True).connect().close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def store():
    if not _has_azure():
        pytest.skip("no azure key")
    url = _metadata_url()
    if not _can_connect(url):
        pytest.skip(f"metadata DB unreachable: {url}")
    from text2sql.providers import build_embedding
    cfg = load_config()
    embedder = build_embedding(cfg.embedding_provider())
    s = GoldStore(url, embedder)
    s.drop_schema()      # clean slate
    s.ensure_schema()
    yield s
    s.drop_schema()


# ── AST flattening ──────────────────────────────────────────────────────────


def test_ast_flatten_drops_literals() -> None:
    a = flatten_sql_ast("SELECT * FROM students WHERE age > 10")
    b = flatten_sql_ast("SELECT * FROM students WHERE age > 99")
    # Literals get tagged identically; structural shape is the same.
    assert a == b
    assert "T:students" in a
    assert "C:age" in a


def test_ast_flatten_preserves_join_structure() -> None:
    flat = flatten_sql_ast(
        "SELECT s.firstname, sch.nameofinstitution "
        "FROM edfi.student s JOIN edfi.studentschoolassociation ssa "
        "ON s.studentusi = ssa.studentusi "
        "JOIN edfi.school sch ON ssa.schoolid = sch.schoolid"
    )
    assert "T:edfi.student" in flat
    assert "T:edfi.studentschoolassociation" in flat
    assert "T:edfi.school" in flat
    assert flat.count("J:") == 2


# ── CRUD + retrieval ─────────────────────────────────────────────────────────


def test_create_and_get(store: GoldStore) -> None:
    rec = store.create(
        nl_question="How many students are enrolled in each school?",
        sql_text=(
            "SELECT s.nameofinstitution, COUNT(*) FROM edfi.studentschoolassociation ssa "
            "JOIN edfi.school s ON ssa.schoolid = s.schoolid GROUP BY s.nameofinstitution"
        ),
        tables_used=["edfi.StudentSchoolAssociation", "edfi.School"],
    )
    fetched = store.get(rec.id)
    assert fetched is not None
    assert fetched.nl_question == rec.nl_question
    assert "T:edfi.studentschoolassociation" in fetched.sql_ast_flat
    assert len(fetched.embedding_nl) > 0


def test_approval_workflow(store: GoldStore) -> None:
    rec = store.create(
        nl_question="Total enrollment count",
        sql_text="SELECT COUNT(*) FROM edfi.studentschoolassociation",
        tables_used=["edfi.StudentSchoolAssociation"],
        approval_status="pending",
    )
    assert rec.approval_status == "pending"
    approved = store.approve(rec.id, reviewer="alice")
    assert approved is not None
    assert approved.approval_status == "approved"
    assert approved.approved_by == "alice"

    rejected = store.reject(rec.id, reviewer="alice", reason="stale")
    assert rejected is not None
    assert rejected.approval_status == "rejected"


def test_retrieve_top_k_finds_semantically_similar(store: GoldStore) -> None:
    store.create(
        nl_question="How many students per school?",
        sql_text="SELECT * FROM edfi.studentschoolassociation",
        tables_used=["edfi.StudentSchoolAssociation"],
        approval_status="approved",
    )
    store.create(
        nl_question="List staff with their credentials",
        sql_text="SELECT * FROM edfi.staff",
        tables_used=["edfi.Staff"],
        approval_status="approved",
    )
    store.create(
        nl_question="Average daily attendance per school",
        sql_text="SELECT * FROM edfi.studentschoolattendanceevent",
        tables_used=["edfi.StudentSchoolAttendanceEvent"],
        approval_status="approved",
    )
    hits = store.retrieve_top_k("students enrolled in school", k=3)
    assert hits
    # Most-similar match should be the per-school enrollment one (the staff
    # query is unrelated).
    assert "students per school" in hits[0].record.nl_question.lower()


def test_retrieve_only_returns_approved(store: GoldStore) -> None:
    store.create(
        nl_question="Pending question please ignore me",
        sql_text="SELECT 1",
        approval_status="pending",
    )
    hits = store.retrieve_top_k("pending question please ignore me", k=5, approved_only=True)
    assert all("Pending question" not in h.record.nl_question for h in hits)


def test_count_by_status(store: GoldStore) -> None:
    total_before = store.count()
    store.create(
        nl_question="Counter test pending",
        sql_text="SELECT 1", approval_status="pending",
    )
    store.create(
        nl_question="Counter test approved",
        sql_text="SELECT 1", approval_status="approved",
    )
    assert store.count() == total_before + 2
    assert store.count(approval_status="approved") >= 1
    assert store.count(approval_status="pending") >= 1
