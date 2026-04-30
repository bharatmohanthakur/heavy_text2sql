"""Q1 + Q2 — operator-supplied catalog input parsers.

Two CSVs replace the Ed-Fi GitHub artifacts:
  * Schema CSV       (Ranking · Domain · TABLE_SCHEMA · TABLE_NAME ·
                      COLUMN_NAME · Populated)
  * Relationships CSV (FK_Name · Parent_Table · Parent_Column ·
                       Referenced_Table · Referenced_Column ·
                       Parent_Schema · Referenced_Schema)

These tests pin the parsers against the actual operator screenshots
(IMG_0426/0427/0428) so the format never silently drifts.
"""

from __future__ import annotations

import textwrap

import pytest

from text2sql.catalog_inputs import (
    ColumnRow,
    RelationshipsCsvError,
    SchemaCsvError,
    parse_relationships_csv,
    parse_schema_csv,
)
from text2sql.catalog_inputs.schema_csv import (
    distinct_domains,
    group_by_table,
)


# ── Schema CSV — Q1 ─────────────────────────────────────────────────────────


def _schema_csv(body: str) -> str:
    return textwrap.dedent(body).strip() + "\n"


def test_schema_csv_parses_operator_format():
    """Mirrors the columns shown in IMG_0426/0427 verbatim."""
    csv_text = _schema_csv("""
        Ranking,Domain,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,Populated
        0,Descriptor,edfi,AbsenceEventCategoryDescriptor,AbsenceEventCategoryDescriptorId,Yes
        0,Descriptor,edfi,AcademicHonorCategoryDescriptor,AcademicHonorCategoryDescriptorId,Yes
        1,Student,edfi,Student,StudentUSI,Yes
        2,Student,edfi,Student,FirstName,No
    """)
    rows = parse_schema_csv(csv_text)
    assert len(rows) == 4
    assert rows[0] == ColumnRow(
        ranking=0, domain="Descriptor", schema="edfi",
        table="AbsenceEventCategoryDescriptor",
        column="AbsenceEventCategoryDescriptorId", populated=True,
    )
    assert rows[3].populated is False  # Student.FirstName not populated


def test_schema_csv_strips_redundant_schema_prefix_from_table_name():
    """Operator exports sometimes spell TABLE_NAME as 'edfi.MyTable'.
    When the prefix matches TABLE_SCHEMA, drop it; otherwise leave alone
    so a real authoring bug surfaces."""
    csv_text = _schema_csv("""
        Ranking,Domain,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,Populated
        0,Student,edfi,edfi.Student,StudentUSI,Yes
        0,Student,edfi,tpdm.Other,OtherId,Yes
    """)
    rows = parse_schema_csv(csv_text)
    assert rows[0].table == "Student"
    # Mismatched prefix is preserved verbatim — surfaces as an FK miss
    # in downstream join validation rather than being silently rewritten.
    assert rows[1].table == "tpdm.Other"


def test_schema_csv_accepts_alternative_populated_spellings():
    csv_text = _schema_csv("""
        Ranking,Domain,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,Populated
        0,A,s,t,c1,yes
        0,A,s,t,c2,YES
        0,A,s,t,c3,1
        0,A,s,t,c4,true
        0,A,s,t,c5,no
        0,A,s,t,c6,0
        0,A,s,t,c7,
    """)
    rows = parse_schema_csv(csv_text)
    assert [r.populated for r in rows] == [True, True, True, True, False, False, False]


def test_schema_csv_rejects_missing_required_column():
    csv_text = _schema_csv("""
        Ranking,Domain,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME
        0,Student,edfi,Student,StudentUSI
    """)
    with pytest.raises(SchemaCsvError) as ei:
        parse_schema_csv(csv_text)
    assert "populated" in str(ei.value).lower()


def test_schema_csv_rejects_non_integer_ranking():
    csv_text = _schema_csv("""
        Ranking,Domain,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,Populated
        high,Student,edfi,Student,StudentUSI,Yes
    """)
    with pytest.raises(SchemaCsvError, match="Ranking"):
        parse_schema_csv(csv_text)


def test_schema_csv_rejects_blank_schema_table_or_column():
    csv_text = _schema_csv("""
        Ranking,Domain,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,Populated
        0,Student,edfi,,StudentUSI,Yes
    """)
    with pytest.raises(SchemaCsvError):
        parse_schema_csv(csv_text)


def test_schema_csv_tolerates_blank_rows_and_excel_header_casing():
    csv_text = _schema_csv("""
        ranking,DOMAIN,table_schema,table_name,column_name,POPULATED
        0,Student,edfi,Student,StudentUSI,Yes
        ,,,,,
        1,Staff,edfi,Staff,StaffUSI,Yes
    """)
    rows = parse_schema_csv(csv_text)
    assert len(rows) == 2
    assert rows[0].domain == "Student"
    assert rows[1].domain == "Staff"


def test_group_by_table_buckets_columns_in_file_order():
    csv_text = _schema_csv("""
        Ranking,Domain,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,Populated
        0,Student,edfi,Student,StudentUSI,Yes
        0,Student,edfi,Student,FirstName,Yes
        0,Staff,edfi,Staff,StaffUSI,Yes
    """)
    grouped = group_by_table(parse_schema_csv(csv_text))
    assert set(grouped) == {"edfi.Student", "edfi.Staff"}
    assert [c.column for c in grouped["edfi.Student"]] == ["StudentUSI", "FirstName"]


def test_distinct_domains_orders_by_first_seen_ranking():
    """Domain taxonomy is data, not config: distinct values from the
    CSV become THE list of domains, ordered by the operator's intended
    priority (lowest Ranking first)."""
    csv_text = _schema_csv("""
        Ranking,Domain,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,Populated
        2,Staff,edfi,Staff,StaffUSI,Yes
        0,Student,edfi,Student,StudentUSI,Yes
        1,Assessment,edfi,Assessment,AssessmentId,Yes
        2,Student,edfi,Student,FirstName,Yes
    """)
    domains = distinct_domains(parse_schema_csv(csv_text))
    # Student=0 wins over Student=2; sort by (ranking, name).
    assert domains == ["Student", "Assessment", "Staff"]


# ── Relationships CSV — Q2 ──────────────────────────────────────────────────


def _rel_csv(body: str) -> str:
    return textwrap.dedent(body).strip() + "\n"


def test_relationships_csv_parses_operator_format():
    """Mirrors IMG_0428: one FK per row (single-column FK case)."""
    csv_text = _rel_csv("""
        FK_Name,Parent_Table,Parent_Column,Referenced_Table,Referenced_Column,Parent_Schema,Referenced_Schema
        FK_AbsenceEventCa_Descriptor,AbsenceEventCategoryDescriptor,AbsenceEventCategoryDescriptorId,Descriptor,DescriptorId,edfi,edfi
        FK_AcademicHonor_Descriptor,AcademicHonorCategoryDescriptor,AcademicHonorCategoryDescriptorId,Descriptor,DescriptorId,edfi,edfi
    """)
    edges = parse_relationships_csv(csv_text)
    assert len(edges) == 2
    e = edges[0]
    assert e.src_schema == "edfi"
    assert e.src_table == "AbsenceEventCategoryDescriptor"
    assert e.dst_schema == "edfi"
    assert e.dst_table == "Descriptor"
    assert e.constraint_name == "FK_AbsenceEventCa_Descriptor"
    assert e.column_pairs == (("AbsenceEventCategoryDescriptorId", "DescriptorId"),)
    assert not e.is_composite


def test_relationships_csv_groups_composite_fks_by_fk_name():
    """A composite FK spans multiple rows sharing the same FK_Name; the
    parser reassembles them into one FKEdge with the column-pair order
    preserved from the CSV."""
    csv_text = _rel_csv("""
        FK_Name,Parent_Table,Parent_Column,Referenced_Table,Referenced_Column,Parent_Schema,Referenced_Schema
        FK_StudentSchool_Composite,StudentSchoolAssoc,StudentUSI,Student,StudentUSI,edfi,edfi
        FK_StudentSchool_Composite,StudentSchoolAssoc,SchoolId,Student,SchoolId,edfi,edfi
    """)
    edges = parse_relationships_csv(csv_text)
    assert len(edges) == 1
    e = edges[0]
    assert e.is_composite
    assert e.column_pairs == (
        ("StudentUSI", "StudentUSI"),
        ("SchoolId",   "SchoolId"),
    )


def test_relationships_csv_strips_redundant_schema_prefix():
    csv_text = _rel_csv("""
        FK_Name,Parent_Table,Parent_Column,Referenced_Table,Referenced_Column,Parent_Schema,Referenced_Schema
        FK_A,edfi.Foo,FooId,edfi.Bar,BarId,edfi,edfi
    """)
    edges = parse_relationships_csv(csv_text)
    assert edges[0].src_table == "Foo"
    assert edges[0].dst_table == "Bar"


def test_relationships_csv_rejects_inconsistent_composite_rows():
    """If two rows share an FK_Name but disagree on parent/referenced
    table, that's an authoring bug — fail loudly with the line number
    so the operator can fix the source."""
    csv_text = _rel_csv("""
        FK_Name,Parent_Table,Parent_Column,Referenced_Table,Referenced_Column,Parent_Schema,Referenced_Schema
        FK_Conflict,Foo,FooId,Bar,BarId,edfi,edfi
        FK_Conflict,Foo,FooId,DIFFERENT,BarId,edfi,edfi
    """)
    with pytest.raises(RelationshipsCsvError, match="disagree"):
        parse_relationships_csv(csv_text)


def test_relationships_csv_rejects_missing_required_column():
    csv_text = _rel_csv("""
        FK_Name,Parent_Table,Parent_Column,Referenced_Table,Referenced_Column,Parent_Schema
        FK_A,Foo,FooId,Bar,BarId,edfi
    """)
    with pytest.raises(RelationshipsCsvError, match="referenced_schema"):
        parse_relationships_csv(csv_text)


def test_relationships_csv_rejects_blank_fk_name():
    csv_text = _rel_csv("""
        FK_Name,Parent_Table,Parent_Column,Referenced_Table,Referenced_Column,Parent_Schema,Referenced_Schema
        ,Foo,FooId,Bar,BarId,edfi,edfi
    """)
    with pytest.raises(RelationshipsCsvError, match="FK_Name"):
        parse_relationships_csv(csv_text)


def test_relationships_csv_rejects_blank_endpoint_columns():
    csv_text = _rel_csv("""
        FK_Name,Parent_Table,Parent_Column,Referenced_Table,Referenced_Column,Parent_Schema,Referenced_Schema
        FK_A,Foo,,Bar,BarId,edfi,edfi
    """)
    with pytest.raises(RelationshipsCsvError, match="non-empty"):
        parse_relationships_csv(csv_text)


def test_relationships_csv_preserves_group_order_across_file():
    """Two distinct FKs interleaved in the file should still come back
    in first-seen order (matters when the catalog builder hashes by
    constraint_name later)."""
    csv_text = _rel_csv("""
        FK_Name,Parent_Table,Parent_Column,Referenced_Table,Referenced_Column,Parent_Schema,Referenced_Schema
        FK_B,X,Xid,Y,Yid,edfi,edfi
        FK_A,P,Pid,Q,Qid,edfi,edfi
        FK_B,X,Xid2,Y,Yid2,edfi,edfi
    """)
    edges = parse_relationships_csv(csv_text)
    assert [e.constraint_name for e in edges] == ["FK_B", "FK_A"]
    assert edges[0].column_pairs == (("Xid", "Yid"), ("Xid2", "Yid2"))
