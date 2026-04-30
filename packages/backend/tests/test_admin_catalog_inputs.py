"""Q3 — admin endpoints for operator-CSV uploads.

Locks the contract for `GET /admin/catalog_inputs` and the multipart
`POST /admin/catalog_inputs/upload`. The CSVs themselves are validated
in test_csv_inputs.py — these tests just prove the HTTP plumbing
(per-provider write location, validate-before-persist, summary counts)
behaves the way the Settings UI expects.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import text2sql.api.admin as admin_mod
from text2sql.api.admin import router


@pytest.fixture
def csv_dir(tmp_path: Path, monkeypatch) -> Path:
    """Redirect `_catalog_inputs_dir` at the per-test temp dir so the
    real per-provider artifact tree never gets touched. Doing this at
    the helper level rather than mocking REPO_ROOT keeps the test
    surface small."""
    target = tmp_path / "catalog_inputs"
    monkeypatch.setattr(admin_mod, "_catalog_inputs_dir", lambda: target)
    return target


@pytest.fixture
def client(csv_dir) -> TestClient:
    a = FastAPI()
    a.include_router(router)
    return TestClient(a)


_SCHEMA_OK = textwrap.dedent("""\
    Ranking,Domain,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,Populated
    0,Student,edfi,Student,StudentUSI,Yes
    0,Student,edfi,Student,FirstName,Yes
    0,EducationOrg,edfi,School,SchoolId,Yes
""")
_REL_OK = textwrap.dedent("""\
    FK_Name,Parent_Table,Parent_Column,Referenced_Table,Referenced_Column,Parent_Schema,Referenced_Schema
    FK_S_Sch,Student,SchoolId,School,SchoolId,edfi,edfi
""")


def test_get_catalog_inputs_reports_absent_before_upload(client):
    r = client.get("/admin/catalog_inputs")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["present"] is False
    assert body["table_count"] is None


def test_upload_persists_and_summarizes(client, csv_dir: Path):
    files = {
        "schema_csv": ("schema.csv", _SCHEMA_OK, "text/csv"),
        "relationships_csv": ("rels.csv", _REL_OK, "text/csv"),
    }
    r = client.post("/admin/catalog_inputs/upload", files=files)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["present"] is True
    # Two tables on the schema side (Student + School)
    assert body["table_count"] == 2
    assert body["column_count"] == 3
    assert body["fk_count"] == 1
    # Files should land in the redirected dir
    assert (csv_dir / "schema.csv").exists()
    assert (csv_dir / "relationships.csv").exists()

    # GET now reflects the upload
    r2 = client.get("/admin/catalog_inputs")
    assert r2.status_code == 200
    assert r2.json()["present"] is True
    assert r2.json()["table_count"] == 2


def test_upload_rejects_invalid_schema_csv_with_400(client, csv_dir: Path):
    bad = "Ranking,Domain\n0,Student\n"  # missing required columns
    files = {
        "schema_csv": ("schema.csv", bad, "text/csv"),
        "relationships_csv": ("rels.csv", _REL_OK, "text/csv"),
    }
    r = client.post("/admin/catalog_inputs/upload", files=files)
    assert r.status_code == 400, r.text
    # Validate-before-persist contract: the bad upload must not have
    # half-written either file to disk.
    assert not (csv_dir / "schema.csv").exists()
    assert not (csv_dir / "relationships.csv").exists()


def test_upload_rejects_non_utf8_with_400(client, csv_dir: Path):
    # Latin-1 byte that's not valid UTF-8
    files = {
        "schema_csv": ("schema.csv", b"\xff\xfeRanking\n", "text/csv"),
        "relationships_csv": ("rels.csv", _REL_OK.encode("utf-8"), "text/csv"),
    }
    r = client.post("/admin/catalog_inputs/upload", files=files)
    assert r.status_code == 400
    assert "UTF-8" in r.json()["detail"]
